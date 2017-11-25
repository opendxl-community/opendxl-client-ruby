require 'socket'
require 'thread'
require 'dxlclient/broker_connection_time'
require 'dxlclient/logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # rubocop:disable ClassLength

  # Worker thread logic for handling broker connections
  class ConnectionWorker
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

    # @param connection_manager [DXLClient::ConnectionManager]
    # @param mqtt_client [MQTT::Client]
    # @param config [DXLClient::Config]
    def initialize(connection_manager, mqtt_client, config, client_object_id)
      @logger = DXLClient::Logger.logger(self.class.name)
      @manager = connection_manager
      @mqtt_client = mqtt_client
      @config = config
      @client_object_id = client_object_id

      @current_broker = nil
      @current_broker_lock = Mutex.new

      @on_connect_callbacks = Set.new

      @connect_request_tries_remaining = @config.connect_retries
      @connect_retry_delay = @config.reconnect_delay
    end

    def add_connect_callback(callback)
      @manager.connect_lock.synchronize do
        @on_connect_callbacks.add(callback)
      end
    end

    # @return [DXLClient::Broker]
    def current_broker
      @current_broker_lock.synchronize { @current_broker }
    end

    def run
      @manager.connect_lock.synchronize do
        begin
          until @manager.connect_request == REQUEST_SHUTDOWN
            run_until_disconnected_or_shutdown
          end
        ensure
          connect_thread_shutdown
        end
      end
    end

    private

    def run_until_disconnected_or_shutdown
      until @manager.connect_request == REQUEST_SHUTDOWN
        process_connect_thread_request
      end
    # disconnection errors
    rescue MQTT::Exception => e
      handle_connection_dropped(e)
    end

    def process_connect_thread_request
      if @manager.connect_request == REQUEST_DISCONNECT
        do_disconnect
      elsif @manager.connect_request == REQUEST_CONNECT ||
            @manager.connect_state == RECONNECTING
        do_connect
      end

      process_connect_thread_response
    end

    def process_connect_thread_response
      if @manager.connect_state == RECONNECTING ||
         should_retry_connection_for_active_request?
        retry_connection
      else
        @manager.connect_response_condition.broadcast
        if @manager.connect_request != REQUEST_SHUTDOWN
          @manager.connect_request_condition.wait(@manager.connect_lock)
        end
      end
    end

    def should_retry_connection_for_active_request?
      @manager.connect_request == REQUEST_CONNECT &&
        @manager.connect_state == NOT_CONNECTED &&
        !@connect_request_tries_remaining.zero?
    end

    def retry_connection
      if @connect_retry_delay > @config.reconnect_delay_max
        @connect_retry_delay = @config.reconnect_delay_max
      end
      @connect_retry_delay += (@config.reconnect_delay_random *
        @connect_retry_delay * rand)

      @logger.errorf('Retrying connection in %s seconds.',
                     @connect_retry_delay)

      @manager.connect_request_condition.wait(@manager.connect_lock,
                                              @connect_retry_delay)

      @connect_retry_delay *= @config.reconnect_back_off_multiplier
    end

    def current_broker=(broker)
      @current_broker_lock.synchronize { @current_broker = broker }
    end

    def do_connect
      @logger.info('Checking ability to connect to brokers...')
      brokers = BrokerConnectionTime.brokers_by_connection_time(
        @config.brokers, @client_object_id
      )

      @logger.info('Trying to connect...')
      if connect_to_broker_from_list(brokers)
        handle_connected_to_broker
      else
        handle_failure_to_connect_to_any_brokers
      end
    end

    def connect_to_broker_from_list(brokers)
      @manager.connect_error = connect_error = nil

      connected_broker = brokers.find do |_, host, broker|
        connected, connect_error = connect_to_next_broker(host, broker.port)
        break if @manager.connect_request == REQUEST_SHUTDOWN
        connected
      end

      self.current_broker = connected_broker ? connected_broker[2] : nil
      @manager.connect_error = connect_error

      connected_broker ? true : false
    end

    def connect_to_next_broker(host, port)
      # Sleep briefly to allow a pending shutdown request to interrupt the
      # connect request. Sleep currently needs to be greater than 0 to avoid an
      # infinite sleep on JRuby.
      # See https://github.com/jruby/jruby/issues/4862.
      @manager.connect_lock.sleep(0.001)
      if @manager.connect_request == REQUEST_SHUTDOWN
        handle_shutdown_during_connect
      else
        begin
          connect_to_broker(host, port)
        rescue StandardError => e
          handle_failure_to_connect_to_one_broker(host, port, e)
        end
      end
    end

    def connect_to_broker(host, port)
      @mqtt_client.host = host
      @mqtt_client.port = port
      @logger.debug("Connecting to broker: #{host}:#{port}...")
      @mqtt_client.connect
      @logger.info("Connected to broker: #{host}:#{port}")
      [true, nil]
    end

    def handle_failure_to_connect_to_one_broker(host, port, exception)
      @logger.error(
        "Failed to connect to #{host}:#{port}: #{exception.message}"
      )
      [false, exception]
    end

    def handle_shutdown_during_connect
      @logger.info('Client shutdown in progress, aborting connect attempt')
      if @manager.connect_state == RECONNECTING
        @manager.connect_state = NOT_CONNECTED
      end
      [false, SocketError.new('Failed to connect, client has been shutdown')]
    end

    def handle_connected_to_broker
      @manager.connect_state = CONNECTED
      if @manager.connect_request == REQUEST_CONNECT
        @manager.connect_request = REQUEST_NONE
      end
      @connect_retry_delay = @config.reconnect_delay
      invoke_on_connect_callbacks
    end

    def invoke_on_connect_callbacks
      @on_connect_callbacks.each do |callback|
        begin
          callback.call
        rescue StandardError => e
          @logger.exception(e, 'Error raised by connect callback')
        end
      end
    end

    def handle_failure_to_connect_to_any_brokers
      unless @manager.connect_state == RECONNECTING
        @manager.connect_state = NOT_CONNECTED
      end
      @manager.connect_error ||=
        SocketError.new('Unable to connect to any brokers')

      if @connect_request_tries_remaining.zero? && @config.connect_retries > 0
        @connect_request_tries_remaining = @config.connect_retries - 1
      elsif @connect_request_tries_remaining > 0
        @connect_request_tries_remaining -= 1
      end
    end

    def do_disconnect
      @logger.info('Disconnecting from broker...')
      @connect_request_tries_remaining = 0
      @mqtt_client.disconnect
      self.current_broker = nil
      @manager.connect_state = NOT_CONNECTED
      if @manager.connect_request == REQUEST_DISCONNECT
        @manager.connect_request = REQUEST_NONE
      end
    rescue StandardError => e
      handle_failure_to_disconnect(e)
    end

    def handle_failure_to_disconnect(exception)
      @logger.debug("Failed to disconnect from broker: #{exception.message}")
      @manager.connect_error = exception
      @manager.connect_state = UNKNOWN
    end

    # @param mqtt_exception [MQTT::Exception]
    def handle_connection_dropped(mqtt_exception)
      self.current_broker = nil
      if @manager.connect_state == CONNECTED &&
         ![REQUEST_DISCONNECT, REQUEST_SHUTDOWN].include?(
           @manager.connect_request
         ) && @config.reconnect_when_disconnected
        setup_reconnect_state_for_dropped_connection(mqtt_exception)
      else
        handle_no_reconnects_for_dropped_connection(mqtt_exception)
      end
    end

    def setup_reconnect_state_for_dropped_connection(mqtt_exception)
      @manager.connect_state = RECONNECTING
      @logger.errorf('Connection error: %s, retrying connection',
                     mqtt_exception.message)
    end

    def handle_no_reconnects_for_dropped_connection(mqtt_exception)
      unless @manager.connect_state == SHUTDOWN
        @manager.connect_state = NOT_CONNECTED
      end
      if @manager.connect_request == REQUEST_CONNECT
        @manager.connect_request = REQUEST_NONE
      end
      send_error_for_dropped_connection(mqtt_exception)
    end

    def send_error_for_dropped_connection(mqtt_exception)
      error = format('Connection error: %s, not retrying connection',
                     mqtt_exception.message)
      @logger.errorf(error)
      @manager.connect_error = SocketError.new(error)
      @manager.connect_response_condition.broadcast
    end

    def connect_thread_shutdown
      do_disconnect
    ensure
      @manager.connect_state = SHUTDOWN
      @manager.connect_request = REQUEST_NONE
      @manager.connect_response_condition.broadcast
      @logger.debug('Connection thread terminating')
    end
  end

  private_constant :ConnectionWorker
end
