require 'mqtt'
require 'dxlclient/logger'

module DXLClient
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
            port: port,
            version: MQTT_VERSION,
            clean_session: true,
            ssl: true,
            keep_alive: config.keep_alive_interval)
      self.cert_file = config.cert_file
      self.key_file = config.private_key
      self.ca_file = config.broker_ca_bundle

      @current_broker = nil

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

      @services_ttl_thread = Thread.new do
        Thread.current.name = 'DXLMQTTClientConnection'
        connect_loop
      end
    end

    def destroy
      @logger.debug('Destroying MQTT client...')
      @connect_lock.synchronize do
        @connect_request = REQUEST_SHUTDOWN
        @connect_request_condition.signal
      end
      @services_ttl_thread.join
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
          if @connect_state == SHUTDOWN
            raise SocketError, 'Failed to connect, client has been shutdown'
          end
          case @connect_request
          when REQUEST_DISCONNECT
            raise SocketError, 'Failed to connect, disconnect in process'
          when REQUEST_SHUTDOWN
            raise SocketError, 'Failed to connect, shutdown in process'
          else
            @connect_request = REQUEST_CONNECT
            @connect_request_condition.signal
            @connect_response_condition.wait(@connect_lock)
            raise @connect_error if @connect_error
          end
        end
      end
    end

    def disconnect
      @logger.debug('Received disconnect call')
      @connect_lock.synchronize do
        until @connect_state == NOT_CONNECTED || @connect_state == SHUTDOWN
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
      end
    end

    private

    def connect_loop
      @logger.debug('Connection thread started')
      @connect_lock.synchronize do
        begin
          until @connect_request == REQUEST_SHUTDOWN
            begin
              until @connect_request == REQUEST_SHUTDOWN
                connect_loop_main
              end
            # disconnection errors
            rescue MQTT::Exception => e
              if @connect_state == CONNECTED &&
                 ![REQUEST_DISCONNECT,
                   REQUEST_SHUTDOWN].include?(@connect_request)
                @connect_state = RECONNECTING
                @logger.errorf('Connection error: %s, retrying connection',
                               e.message)
              else
                @logger.debugf('Connection error, not retrying connection')
              end
            end
          end
        ensure
          begin
            do_disconnect
          ensure
            @connect_state = SHUTDOWN
            @connect_request = REQUEST_NONE
            @connect_response_condition.broadcast
            @logger.debug('Connection thread terminating')
          end
        end
      end
    end

    def connect_loop_main
      if @connect_request == REQUEST_DISCONNECT
        do_disconnect
        @connect_request_tries_remaining = 0
      elsif @connect_request == REQUEST_CONNECT ||
          @connect_state == RECONNECTING
        do_connect
        if @connect_request == REQUEST_CONNECT
          if @connect_request_tries_remaining.zero? && @config.connect_retries > 0
            @connect_request_tries_remaining = @config.connect_retries - 1
          elsif @connect_request_tries_remaining > 0
            @connect_request_tries_remaining -= 1
          end
        end
      end

      if @connect_state == RECONNECTING ||
          (@connect_request == REQUEST_CONNECT &&
              @connect_state == NOT_CONNECTED &&
              !@connect_request_tries_remaining.zero?)
        error = @connect_error ? ": #{@connect_error.message}" : ''

        if @connect_retry_delay > @config.reconnect_delay_max
          @connect_retry_delay = @config.reconnect_delay_max
        end
        @connect_retry_delay += (@config.reconnect_delay_random *
            @connect_retry_delay * rand)

        @logger.errorf(
            'Retrying connection in %s seconds. Connection error%s',
            @connect_retry_delay, error)

        @connect_request_condition.wait(@connect_lock, @connect_retry_delay)

        @connect_retry_delay *= @config.reconnect_back_off_multiplier
      else
        @connect_response_condition.broadcast
        @connect_request_condition.wait(@connect_lock)
      end
    end

    def do_connect
      @connect_error = nil
      @logger.debug('Connecting to broker from connect thread...')

      @current_broker = @config.brokers.find do |broker|
        broker.hosts.find do |host|
          self.host = host
          begin
            @last_ping_response = Time.now
            mqtt_connect
            @logger.info("Connected to broker: #{host}")
            host
          rescue StandardError => e
            @logger.error("Failed to connect to #{host}: #{e.message}")
            @connect_error = e
            nil
          end
        end
      end

      if @current_broker.nil?
        @connect_state = NOT_CONNECTED unless @connect_state == RECONNECTING
        @connect_error ||= SocketError.new('Unable to connect to any brokers')
      else
        @connect_state = CONNECTED
        @connect_retry_delay = @config.reconnect_delay
        @on_connect_callbacks.each do |callback|
          begin
            callback.call
          rescue StandardError => e
            @logger.exception(e,
                              'Error raised by connect callback')
          end
        end
      end
    end

    def do_disconnect
      @logger.debug('Disconnecting from broker from connect thread...')
      mqtt_disconnect
      @connect_state = NOT_CONNECTED
    rescue StandardError => e
      @logger.debug("Failed to disconnect from #{host}: #{e.message}")
      @connect_error = e
      @connect_state = UNKNOWN
    end

    def handle_packet(packet)
      if packet.class == MQTT::Packet::Publish
        @on_publish_callbacks.each do |callback|
          begin
            callback.call(packet)
          rescue StandardError => e
            @logger.exception(e,
                              'Error raised by publish callback')
          end
        end
      else
        super(packet)
      end
    end
  end

  private_constant :MQTTClient
end
