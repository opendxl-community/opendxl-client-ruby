require 'socket'
require 'thread'
require 'dxlclient/connection_worker'
require 'dxlclient/logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Manager which handles connections to brokers on the DXL fabric. The manager
  # establishes connections and handles reconnecting to a broker in the event
  # that a connection is dropped.
  class ConnectionManager
    attr_accessor :connect_error, :connect_request, :connect_state

    attr_reader :connect_lock, :connect_request_condition,
                :connect_response_condition

    # rubocop: disable AbcSize, MethodLength

    # @param config [DXLClient::Config]
    # @param mqtt_client [MQTT::Client]
    def initialize(config, mqtt_client)
      @logger = DXLClient::Logger.logger(self.class.name)
      @config = config
      @mqtt_client = mqtt_client

      if config.brokers.nil? || config.brokers.empty?
        raise ArgumentError, 'No brokers in configuration so cannot connect'
      end

      @connect_lock = Mutex.new
      @connect_request_condition = ConditionVariable.new
      @connect_response_condition = ConditionVariable.new
      @connect_error = nil
      @connect_state = ConnectionWorker::NOT_CONNECTED
      @connect_request = ConnectionWorker::REQUEST_NONE

      @worker = ConnectionWorker.new(self, mqtt_client, @config)

      @connect_thread = Thread.new do
        Thread.current.name = 'DXLConnectionManager'
        @logger.debug('Connection thread started')
        @worker.run
      end
    end

    # rubocop: enable AbcSize, MethodLength

    # Tear down any active connection and any other resources used by the
    # connection manager.
    def destroy
      @logger.debug('Destroying connection manager...')
      @connect_lock.synchronize do
        @connect_request = ConnectionWorker::REQUEST_SHUTDOWN
        @connect_request_condition.signal
      end
      @connect_thread.join
      @logger.debug('Connection manager destroyed')
    end

    def add_connect_callback(callback)
      @worker.add_connect_callback(callback)
    end

    def connect
      @logger.debug('Received connect call')
      @connect_lock.synchronize do
        until @connect_state == ConnectionWorker::CONNECTED
          if @connect_state == ConnectionWorker::SHUTDOWN ||
             @connect_request == ConnectionWorker::REQUEST_SHUTDOWN
            raise SocketError, 'Failed to connect, client has been shutdown'
          end
          handle_connect_request
        end
      end
    end

    def disconnect
      @logger.debug('Received disconnect call')
      @connect_lock.synchronize do
        until @connect_state == ConnectionWorker::NOT_CONNECTED ||
              @connect_state == ConnectionWorker::SHUTDOWN
          handle_disconnect_request
        end
      end
    end

    # @return [DXLClient::Broker]
    def current_broker
      @worker.current_broker
    end

    private

    def handle_connect_request
      raise SocketError, 'Failed to connect, disconnect in process' \
        if @connect_request == ConnectionWorker::REQUEST_DISCONNECT
      @connect_request = ConnectionWorker::REQUEST_CONNECT
      @connect_request_condition.signal
      @connect_response_condition.wait(@connect_lock)
      raise @connect_error if @connect_error
    end

    def handle_disconnect_request
      if @connect_request == ConnectionWorker::REQUEST_CONNECT
        raise SocketError, 'Failed to disconnect, connect in process'
      end
      unless @connect_request == ConnectionWorker::REQUEST_SHUTDOWN
        @connect_request = ConnectionWorker::REQUEST_DISCONNECT
      end
      @connect_request_condition.signal
      @connect_response_condition.wait(@connect_lock)
      raise @connect_error if @connect_error
    end
  end

  private_constant :ConnectionManager
end
