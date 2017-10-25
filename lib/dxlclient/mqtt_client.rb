require 'mqtt'
require 'dxlclient/logger'

module DXLClient
  class MQTTClient < MQTT::Client
    MQTT_VERSION = '3.1.1'

    attr_accessor :on_message

    # @param config [DXLClient::Config]
    def initialize(config)
      @config = config
      @logger = DXLClient::Logger.logger(self.class)

      if config.brokers.nil? || config.brokers.length == 0
        raise ArgumentError, 'No brokers in configuration so cannot connect'
      end

      super(:client_id => config.client_id,
            :port => port,
            :version => MQTT_VERSION,
            :clean_session => true,
            :ssl => true)
      self.cert_file = config.cert_file
      self.key_file = config.private_key
      self.ca_file = config.broker_ca_bundle
    end

    def connect
      connect_error = nil

      @logger.info('Trying to connect...')

      @current_broker = @config.brokers.find do |broker|
        broker.hosts.find do |host|
          self.host = host
          begin
            super
            host
          rescue StandardError => e
            @logger.debug("Failed to connect to #{host}: #{e.message}")
            connect_error = e
            nil
          end
        end
      end

      if @current_broker.nil?
        if connect_error
          raise connect_error
        else
          raise SocketError, 'Unable to connect to any brokers'
        end
      end
    end

    private

    def handle_packet(packet)
      if packet.class == MQTT::Packet::Publish
        if on_message
          on_message.call(packet)
        end
      else
        super(packet)
      end
    end
  end

  private_constant :MQTTClient
end
