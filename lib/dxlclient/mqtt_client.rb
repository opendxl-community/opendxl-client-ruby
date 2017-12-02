require 'mqtt'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Subclass of the {MQTT:Client} class which provides custom MQTT client
  # handling for DXL - for example, dispatching callbacks for message publish
  # events.
  class MQTTClient < MQTT::Client
    alias mqtt_connect connect

    def initialize(*)
      @on_message_lock = Mutex.new
      @on_message = nil
      super
    end

    # Register a callback to be invoked with the content of a published message
    def on_message(callback)
      @on_message_lock.synchronize { @on_message = callback }
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
        @on_message_lock.synchronize do
          @on_message ? @on_message.call(packet) : super(packet)
        end
      else
        super(packet)
      end
    end
  end

  private_constant :MQTTClient
end
