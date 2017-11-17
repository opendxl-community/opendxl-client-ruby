require 'socket'
require 'mqtt'
require 'dxlclient/logger'

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

    CHECK_CONNECTION_TIMEOUT = 1

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

    # @return [DXLClient::Broker]
    def current_broker
      @current_broker_lock.synchronize { @current_broker }
    end

    private

    def connect_loop
      @logger.debug('Connection thread started')
      @connect_lock.synchronize do
        begin
          until @connect_request == REQUEST_SHUTDOWN
            begin
              connect_loop_main until @connect_request == REQUEST_SHUTDOWN
            # disconnection errors
            rescue MQTT::Exception => e
              self.current_broker = nil
              if @connect_state == CONNECTED &&
                 ![REQUEST_DISCONNECT, REQUEST_SHUTDOWN].include?(
                   @connect_request
                 ) && @config.reconnect_when_disconnected
                @connect_state = RECONNECTING
                @logger.errorf('Connection error: %s, retrying connection',
                               e.message)
              else
                @connect_state = NOT_CONNECTED unless @connect_state == SHUTDOWN
                if @connect_request == REQUEST_CONNECT
                  @connect_request = REQUEST_NONE
                end
                error = format('Connection error: %s, not retrying connection',
                               e.message)
                @logger.errorf(error)
                @connect_error = SocketError.new(error)
                @connect_response_condition.broadcast
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
      elsif @connect_request == REQUEST_CONNECT ||
            @connect_state == RECONNECTING
        do_connect
      end

      if @connect_state == RECONNECTING ||
         (@connect_request == REQUEST_CONNECT &&
          @connect_state == NOT_CONNECTED &&
          !@connect_request_tries_remaining.zero? &&
          @config.reconnect_when_disconnected)
        if @connect_retry_delay > @config.reconnect_delay_max
          @connect_retry_delay = @config.reconnect_delay_max
        end
        @connect_retry_delay += (@config.reconnect_delay_random *
            @connect_retry_delay * rand)

        @logger.errorf('Retrying connection in %s seconds.',
                       @connect_retry_delay)

        @connect_request_condition.wait(@connect_lock, @connect_retry_delay)

        @connect_retry_delay *= @config.reconnect_back_off_multiplier
      else
        @connect_response_condition.broadcast
        if @connect_request != REQUEST_SHUTDOWN
          @connect_request_condition.wait(@connect_lock)
        end
      end
    end

    def connect_to_broker(host, broker, timeout = CHECK_CONNECTION_TIMEOUT)
      connected = false
      start = Time.now

      begin
        addr_info = Socket.getaddrinfo(host, nil)
      rescue SocketError => e
        @logger.errorf('Failed to get info for broker host %s: %s',
                       host, e.message)
      end

      if addr_info
        sockaddr = Socket.pack_sockaddr_in(broker.port, host)
        socket = Socket.new(Socket.const_get(addr_info[0][0]),
                            Socket::SOCK_STREAM, 0)
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
        @logger.debug(
          "Checking connection time for broker: #{host}:#{broker.port}..."
        )
        begin
          socket.connect_nonblock(sockaddr)
          connected = true
        rescue IO::WaitWritable, Errno::EAGAIN
          if IO.select(nil, [socket], nil, timeout)
            begin
              socket.connect_nonblock(sockaddr)
              connected = true
            rescue Errno::EISCONN
              connected = true
            end
          end
          unless connected
            @logger.errorf('Timed out trying to connect to broker %s:%d',
                           host, broker.port)
          end
        rescue Errno::ECONNREFUSED => e
          @logger.errorf('Failed to connect to broker %s:%d: %s',
                         host, broker.port, e.message)
        ensure
          socket.close
        end
      end

      connected ? Time.now - start : nil
    end

    def current_broker=(broker)
      @current_broker_lock.synchronize { @current_broker = broker }
    end

    def brokers_by_connection_time
      hosts_to_broker = @config.brokers.each_with_object({}) do |broker, acc|
        broker.hosts.each { |host| acc["#{broker.port}:#{host}"] = broker }
      end

      host_connection_threads = hosts_to_broker.collect do |port_host, broker|
        host = port_host.split(':')[-1]
        Thread.new do
          Thread.current.name = "DXLMQTTClientHostCheck-#{host}"
          [connect_to_broker(host, broker), host, broker]
        end
      end

      broker_connection_times = host_connection_threads.collect do |thread|
        thread.join
        thread.value
      end

      broker_connection_times.sort do |a, b|
        if a[0].nil?
          1
        elsif b[0].nil?
          -1
        else
          a[0] <=> b[0]
        end
      end
    end

    def do_connect
      @connect_error = connect_error = nil

      @logger.debug('Checking brokers...')
      brokers = brokers_by_connection_time

      @logger.info('Trying to connect...')
      connected_broker = brokers.find do |_, host, broker|
        @connect_lock.sleep(0)
        if @connect_request == REQUEST_SHUTDOWN
          @logger.info(
            'Client shutdown in progress, aborting connect attempt'
          )
          @connect_state = NOT_CONNECTED if @connect_state == RECONNECTING
          connect_error = SocketError.new(
            'Failed to connect, client has been shutdown'
          )
          break
        end
        self.host = host
        self.port = broker.port
        begin
          @last_ping_response = Time.now
          @logger.debug("Connecting to broker: #{host}:#{broker.port}...")
          mqtt_connect
          @logger.info("Connected to broker: #{host}:#{broker.port}")
          connect_error = nil
          true
        rescue StandardError => e
          @logger.error(
            "Failed to connect to #{host}:#{broker.port}: #{e.message}"
          )
          connect_error = e
          false
        end
      end

      self.current_broker = connected_broker ? connected_broker[2] : nil
      @connect_error = connect_error

      if connected_broker
        @connect_state = CONNECTED
        @connect_request = REQUEST_NONE if @connect_request == REQUEST_CONNECT
        @connect_retry_delay = @config.reconnect_delay
        @on_connect_callbacks.each do |callback|
          begin
            callback.call
          rescue StandardError => e
            @logger.exception(e,
                              'Error raised by connect callback')
          end
        end
      else
        @connect_state = NOT_CONNECTED unless @connect_state == RECONNECTING
        @connect_error ||= SocketError.new('Unable to connect to any brokers')

        if @connect_request_tries_remaining.zero? && @config.connect_retries > 0
          @connect_request_tries_remaining = @config.connect_retries - 1
        elsif @connect_request_tries_remaining > 0
          @connect_request_tries_remaining -= 1
        end
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
