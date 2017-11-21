require 'socket'
require 'thread'
require 'dxlclient/logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # rubocop:disable ModuleLength,

  # Module with helpers for checking the amount of time needed to connect to
  # a broker.
  module BrokerConnectionTime
    @logger = nil

    CHECK_CONNECTION_TIMEOUT = 1

    private_constant :CHECK_CONNECTION_TIMEOUT

    class << self
      def brokers_by_connection_time(brokers)
        hosts_to_broker = brokers.each_with_object({}) do |broker, acc|
          broker.hosts.each { |host| acc["#{broker.port}:#{host}"] = broker }
        end

        broker_info_sorted_by_connection_time(
          broker_connection_threads(hosts_to_broker).collect do |thread|
            thread.join
            thread.value
          end
        )
      end

      def logger
        @logger ||= DXLClient::Logger.logger(name)
      end

      private

      def broker_connection_threads(hosts_to_broker)
        hosts_to_broker.collect do |port_host, broker|
          host = port_host.split(':')[-1]
          Thread.new do
            Thread.current.name = "DXLBrokerConnectionTime-#{host}"
            [get_broker_connection_time(host, broker.port), host, broker]
          end
        end
      end

      def broker_info_sorted_by_connection_time(connection_times)
        connection_times.sort do |a, b|
          if a[0].nil?
            1
          elsif b[0].nil?
            -1
          else
            a[0] <=> b[0]
          end
        end
      end

      def get_broker_connection_time(host, port)
        connect_successful = false
        start = Time.now
        addr_info = get_host_addr_info(host)

        if addr_info
          logger.debug(
            "Checking connection time for broker: #{host}:#{port}..."
          )
          connect_successful = can_connect_to_broker?(addr_info, host, port)
        end

        connect_successful ? Time.now - start : nil
      end

      def get_host_addr_info(host)
        addr_info = nil
        begin
          addr_info = Socket.getaddrinfo(host, nil)
        rescue SocketError => e
          logger.errorf('Failed to get info for broker host %s: %s',
                        host, e.message)
        end
        addr_info
      end

      def can_connect_to_broker?(addr_info, host, port)
        with_socket(addr_info, host, port) do |socket, sock_addr|
          begin
            socket.connect_nonblock(sock_addr)
            true
          rescue IO::WaitWritable, Errno::EAGAIN
            can_connect_to_broker_within_timeout?(socket, sock_addr, host, port)
          end
        end
      end

      def with_socket(addr_info, host, port)
        socket, sock_addr = create_socket(addr_info, host, port)
        begin
          yield(socket, sock_addr)
        rescue Errno::ECONNREFUSED => e
          logger.errorf('Failed to connect to broker %s:%d: %s',
                        host, port, e.message)
          false
        ensure
          socket.close
        end
      end

      def create_socket(addr_info, host, port)
        socket = Socket.new(Socket.const_get(addr_info[0][0]),
                            Socket::SOCK_STREAM, 0)
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
        sock_addr = Socket.pack_sockaddr_in(port, host)
        [socket, sock_addr]
      end

      def can_connect_to_broker_within_timeout?(
        socket, sock_addr, host, port
      )
        if IO.select(nil, [socket], nil, CHECK_CONNECTION_TIMEOUT)
          broker_connected?(socket, sock_addr)
        else
          logger.errorf('Timed out trying to connect to broker %s:%d',
                        host, port)
          false
        end
      end

      def broker_connected?(socket, sock_addr)
        socket.connect_nonblock(sock_addr)
        true
      rescue Errno::EISCONN
        true
      end
    end
  end

  private_constant :BrokerConnectionTime
end
