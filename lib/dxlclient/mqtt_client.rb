require 'mqtt'
require 'dxlclient/logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Subclass of the {MQTT:Client} class which provides custom MQTT client
  # handling for DXL - for example, dispatching callbacks for message publish
  # events.
  class MQTTClient < MQTT::Client
    MQTT_VERSION = '3.1.1'.freeze

    private_constant :MQTT_VERSION

    alias mqtt_connect connect

    # rubocop: disable MethodLength

    # @param config [DXLClient::Config]
    def initialize(config)
      @config = config
      @logger = DXLClient::Logger.logger(self.class.name)

      super(client_id: config.client_id,
            version: MQTT_VERSION,
            clean_session: true,
            ssl: true,
            keep_alive: config.keep_alive_interval)
      self.cert_file = config.cert_file
      self.key_file = config.private_key
      self.ca_file = config.broker_ca_bundle

      @publish_lock = Mutex.new
      @on_publish_callbacks = Set.new
    end
    # rubocop: enable MethodLength

    # Register a callback to be invoked with the content of a published message
    def add_publish_callback(callback)
      @publish_lock.synchronize do
        @on_publish_callbacks.add(callback)
      end
    end

    def connect
      # Need to reset the ping response to now so that an un-acked ping from
      # a prior connection on the same client does not cause a ping timeout
      # in the MQTT client.
      # TODO: File a fix/PR with ruby-mqtt to fix this
      @last_ping_response = Time.now
      mqtt_connect
    end

    private

    def handle_packet(packet)
      if packet.class == MQTT::Packet::Publish
        deliver_publish_messages_to_callbacks(packet)
      else
        super(packet)
      end
    end

    def deliver_publish_messages_to_callbacks(packet)
      @publish_lock.synchronize do
        @on_publish_callbacks.each do |callback|
          begin
            callback.call(packet)
          rescue StandardError => e
            @logger.exception(e, 'Error raised by publish callback')
          end
        end
      end
    end
  end

  private_constant :MQTTClient
end
