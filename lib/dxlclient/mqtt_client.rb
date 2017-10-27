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

    CONNECTION_RETRY_INTERVAL = 5

    private_constant :MQTT_VERSION, :NOT_CONNECTED, :CONNECTED,
                     :REQUEST_DISCONNECT, :REQUEST_CONNECT, :REQUEST_NONE,
                     :CONNECTION_RETRY_INTERVAL

    alias mqtt_connect connect
    alias mqtt_disconnect disconnect

    # @param config [DXLClient::Config]
    def initialize(config)
      @config = config
      @logger = DXLClient::Logger.logger(self.class)

      if config.brokers.nil? || config.brokers.empty?
        raise ArgumentError, 'No brokers in configuration so cannot connect'
      end

      super(client_id: config.client_id,
            port: port,
            version: MQTT_VERSION,
            clean_session: true,
            ssl: true)
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
      @connect_thread = Thread.new { connect_loop }
    end

    def destroy
      @connect_lock.synchronize do
        @connect_request = REQUEST_SHUTDOWN
        @connect_request_condition.signal
      end
      @connect_thread.join
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
                if @connect_request == REQUEST_DISCONNECT
                  do_disconnect
                elsif @connect_request == REQUEST_CONNECT ||
                      @connect_state == RECONNECTING
                  do_connect
                end

                if @connect_state == RECONNECTING
                  error = @connect_error ? ": #{@connect_error.message}" : ''
                  @logger.debugf('%s%s, %s %s seconds',
                                 'Connection error',
                                 error, 'retrying connection in',
                                 CONNECTION_RETRY_INTERVAL)
                  @connect_request_condition.wait(@connect_lock,
                                                  CONNECTION_RETRY_INTERVAL)
                else
                  @connect_response_condition.broadcast
                  @connect_request_condition.wait(@connect_lock)
                end
              end
            # disconnection errors
            rescue MQTT::Exception => e
              if @connect_state == CONNECTED &&
                 [REQUEST_CONNECT, REQUEST_NONE].include?(@connect_request)
                @connect_state = RECONNECTING
                @logger.debugf('Connection error: %s, retrying connection',
                               e.message)
              else
                @logger.debug('Connection error, not retrying connection')
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

    def do_connect
      @connect_error = nil
      @logger.debug('Connecting to broker from connect thread...')

      @current_broker = @config.brokers.find do |broker|
        broker.hosts.find do |host|
          self.host = host
          begin
            mqtt_connect
            @logger.info("Connected to broker: #{host}")
            host
          rescue StandardError => e
            @logger.debug("Failed to connect to #{host}: #{e.message}")
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
