require 'socket'
require 'mqtt'
require 'dxlclient/logger'
require 'dxlclient/broker_connection_time'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Subclass of the {MQTT:Client} class which provides custom connection
  # handling for DXL - for example, reconnection on disconnect and dispatching
  # callbacks for connection and message publish events.
  class MQTTClient < MQTT::Client
    MQTT_VERSION = '3.1.1'.freeze

    # Connection state
    NOT_CONNECTED = 0
    CONNECTED = 1
    RECONNECTING = 2
    UNKNOWN = 3
    SHUTDOWN = 4

    # Connection request
    REQUEST_NONE = 0
    REQUEST_CONNECT = 1
    REQUEST_DISCONNECT = 2
    REQUEST_SHUTDOWN = 3

    private_constant :MQTT_VERSION, :NOT_CONNECTED, :CONNECTED,
                     :REQUEST_DISCONNECT, :REQUEST_CONNECT, :REQUEST_NONE

    alias mqtt_connect connect
    alias mqtt_disconnect disconnect

    # @param config [DXLClient::Config]
    def initialize(config)
      @config = config
      @logger = DXLClient::Logger.logger(self.class.name)

      if config.brokers.nil? || config.brokers.empty?
        raise ArgumentError, 'No brokers in configuration so cannot connect'
      end

      super(client_id: config.client_id,
            version: MQTT_VERSION,
            clean_session: true,
            ssl: true,
            keep_alive: config.keep_alive_interval)
      self.cert_file = config.cert_file
      self.key_file = config.private_key
      self.ca_file = config.broker_ca_bundle

      @current_broker = nil
      @current_broker_lock = Mutex.new

      @on_connect_callbacks = Set.new
      @on_publish_callbacks = Set.new

      @connect_lock = Mutex.new
      @connect_request_condition = ConditionVariable.new
      @connect_response_condition = ConditionVariable.new
      @connect_error = nil
      @connect_state = NOT_CONNECTED
      @connect_request = REQUEST_NONE

      @connect_request_tries_remaining = @config.connect_retries
      @connect_retry_delay = @config.reconnect_delay

      @connect_thread = Thread.new do
        Thread.current.name = 'DXLMQTTClient'
        connect_thread_run
      end
    end

    def destroy
      @logger.debug('Destroying MQTT client...')
      @connect_lock.synchronize do
        @connect_request = REQUEST_SHUTDOWN
        @connect_request_condition.signal
      end
      @connect_thread.join
      @logger.debug('MQTT client destroyed')
    end

    def add_connect_callback(callback)
      @connect_lock.synchronize do
        @on_connect_callbacks.add(callback)
      end
    end

    def add_publish_callback(callback)
      @connect_lock.synchronize do
        @on_publish_callbacks.add(callback)
      end
    end

    def connect
      @logger.debug('Received connect call')
      @connect_lock.synchronize do
        until @connect_state == CONNECTED
          if @connect_state == SHUTDOWN || @connect_request == REQUEST_SHUTDOWN
            raise SocketError, 'Failed to connect, client has been shutdown'
          end
          handle_connect_request
        end
      end
    end

    def disconnect
      @logger.debug('Received disconnect call')
      @connect_lock.synchronize do
        until @connect_state == NOT_CONNECTED || @connect_state == SHUTDOWN
          handle_disconnect_request
        end
      end
    end

    # @return [DXLClient::Broker]
    def current_broker
      @current_broker_lock.synchronize { @current_broker }
    end

    private

    def handle_connect_request
      raise SocketError, 'Failed to connect, disconnect in process' \
        if @connect_request == REQUEST_DISCONNECT
      @connect_request = REQUEST_CONNECT
      @connect_request_condition.signal
      @connect_response_condition.wait(@connect_lock)
      raise @connect_error if @connect_error
    end

    def handle_disconnect_request
      if @connect_request == REQUEST_CONNECT
        raise SocketError, 'Failed to disconnect, connect in process'
      end
      unless @connect_request == REQUEST_SHUTDOWN
        @connect_request = REQUEST_DISCONNECT
      end
      @connect_request_condition.signal
      @connect_response_condition.wait(@connect_lock)
      raise @connect_error if @connect_error
    end

    def connect_thread_run
      @logger.debug('Connection thread started')
      @connect_lock.synchronize do
        begin
          until @connect_request == REQUEST_SHUTDOWN
            run_until_disconnected_or_shutdown
          end
        ensure
          connect_thread_shutdown
        end
      end
    end

    def run_until_disconnected_or_shutdown
      process_connect_thread_request until @connect_request == REQUEST_SHUTDOWN
    # disconnection errors
    rescue MQTT::Exception => e
      handle_connection_dropped(e)
    end

    def process_connect_thread_request
      if @connect_request == REQUEST_DISCONNECT
        do_disconnect
      elsif @connect_request == REQUEST_CONNECT ||
            @connect_state == RECONNECTING
        do_connect
      end

      process_connect_thread_response
    end

    def process_connect_thread_response
      if @connect_state == RECONNECTING ||
         should_retry_connection_for_active_request?
        retry_connection
      else
        @connect_response_condition.broadcast
        if @connect_request != REQUEST_SHUTDOWN
          @connect_request_condition.wait(@connect_lock)
        end
      end
    end

    def should_retry_connection_for_active_request?
      @connect_request == REQUEST_CONNECT &&
        @connect_state == NOT_CONNECTED &&
        !@connect_request_tries_remaining.zero? &&
        @config.reconnect_when_disconnected
    end

    def retry_connection
      if @connect_retry_delay > @config.reconnect_delay_max
        @connect_retry_delay = @config.reconnect_delay_max
      end
      @connect_retry_delay += (@config.reconnect_delay_random *
        @connect_retry_delay * rand)

      @logger.errorf('Retrying connection in %s seconds.',
                     @connect_retry_delay)

      @connect_request_condition.wait(@connect_lock, @connect_retry_delay)

      @connect_retry_delay *= @config.reconnect_back_off_multiplier
    end

    def current_broker=(broker)
      @current_broker_lock.synchronize { @current_broker = broker }
    end

    def do_connect
      @logger.info('Checking ability to connect to brokers...')
      brokers = BrokerConnectionTime.brokers_by_connection_time(
        @config.brokers
      )

      @logger.info('Trying to connect...')
      if connect_to_broker_from_list(brokers)
        handle_connected_to_broker
      else
        handle_failure_to_connect_to_any_brokers
      end
    end

    def connect_to_broker_from_list(brokers)
      @connect_error = connect_error = nil

      connected_broker = brokers.find do |_, host, broker|
        connected, connect_error = connect_to_next_broker(host, broker.port)
        break if @connect_request == REQUEST_SHUTDOWN
        connected
      end

      self.current_broker = connected_broker ? connected_broker[2] : nil
      @connect_error = connect_error

      connected_broker ? true : false
    end

    def connect_to_next_broker(host, port)
      @connect_lock.sleep(0)
      if @connect_request == REQUEST_SHUTDOWN
        handle_shutdown_during_connect
      else
        begin
          connect_to_broker(host, port)
        rescue StandardError => e
          handle_failed_broker_connection(host, port, e)
        end
      end
    end

    def connect_to_broker(host, port)
      self.host = host
      self.port = port
      @last_ping_response = Time.now
      @logger.debug("Connecting to broker: #{host}:#{port}...")
      mqtt_connect
      @logger.info("Connected to broker: #{host}:#{port}")
      [true, nil]
    end

    def handle_failed_broker_connection(host, port, exception)
      @logger.error(
        "Failed to connect to #{host}:#{port}: #{exception.message}"
      )
      [false, exception]
    end

    def handle_shutdown_during_connect
      @logger.info('Client shutdown in progress, aborting connect attempt')
      @connect_state = NOT_CONNECTED if @connect_state == RECONNECTING
      [false, SocketError.new('Failed to connect, client has been shutdown')]
    end

    def handle_connected_to_broker
      @connect_state = CONNECTED
      @connect_request = REQUEST_NONE if @connect_request == REQUEST_CONNECT
      @connect_retry_delay = @config.reconnect_delay
      @on_connect_callbacks.each do |callback|
        begin
          callback.call
        rescue StandardError => e
          @logger.exception(e, 'Error raised by connect callback')
        end
      end
    end

    def handle_failure_to_connect_to_any_brokers
      @connect_state = NOT_CONNECTED unless @connect_state == RECONNECTING
      @connect_error ||= SocketError.new('Unable to connect to any brokers')

      if @connect_request_tries_remaining.zero? && @config.connect_retries > 0
        @connect_request_tries_remaining = @config.connect_retries - 1
      elsif @connect_request_tries_remaining > 0
        @connect_request_tries_remaining -= 1
      end
    end

    def do_disconnect
      @logger.info('Disconnecting from broker...')
      @connect_request_tries_remaining = 0
      mqtt_disconnect
      self.current_broker = nil
      @connect_state = NOT_CONNECTED
      @connect_request = REQUEST_NONE if @connect_request == REQUEST_DISCONNECT
    rescue StandardError => e
      @logger.debug("Failed to disconnect from #{host}: #{e.message}")
      @connect_error = e
      @connect_state = UNKNOWN
    end

    # @param mqtt_exception [MQTT::Exception]
    def handle_connection_dropped(mqtt_exception)
      self.current_broker = nil
      if @connect_state == CONNECTED &&
         ![REQUEST_DISCONNECT, REQUEST_SHUTDOWN].include?(
           @connect_request
         ) && @config.reconnect_when_disconnected
        setup_reconnect_state_for_dropped_connection(mqtt_exception)
      else
        send_error_for_dropped_connection(mqtt_exception)
      end
    end

    def setup_reconnect_state_for_dropped_connection(mqtt_exception)
      @connect_state = RECONNECTING
      @logger.errorf('Connection error: %s, retrying connection',
                     mqtt_exception.message)
    end

    def send_error_for_dropped_connection(mqtt_exception)
      @connect_state = NOT_CONNECTED unless @connect_state == SHUTDOWN
      @connect_request = REQUEST_NONE if @connect_request == REQUEST_CONNECT
      error = format('Connection error: %s, not retrying connection',
                     mqtt_exception.message)
      @logger.errorf(error)
      @connect_error = SocketError.new(error)
      @connect_response_condition.broadcast
    end

    def connect_thread_shutdown
      do_disconnect
    ensure
      @connect_state = SHUTDOWN
      @connect_request = REQUEST_NONE
      @connect_response_condition.broadcast
      @logger.debug('Connection thread terminating')
    end

    def handle_packet(packet)
      if packet.class == MQTT::Packet::Publish
        deliver_publish_messages_to_callbacks(packet)
      else
        super(packet)
      end
    end

    def deliver_publish_messages_to_callbacks(packet)
      @on_publish_callbacks.each do |callback|
        begin
          callback.call(packet)
        rescue StandardError => e
          @logger.exception(e, 'Error raised by publish callback')
        end
      end
    end
  end

  private_constant :MQTTClient
end
