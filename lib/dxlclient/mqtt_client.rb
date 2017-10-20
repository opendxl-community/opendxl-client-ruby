require 'mqtt'

module DXLClient
  class MQTTClient < MQTT::Client
    attr_accessor :on_message

    def initialize(*args)
      super(*args)
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
