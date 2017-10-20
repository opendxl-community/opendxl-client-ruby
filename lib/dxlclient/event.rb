require 'dxlclient/message'

module DXLClient
  class Event < Message
    attr_reader :message_type

    def initialize(destination_topic)
      super(destination_topic)
      @message_type = DXLClient::Message::MESSAGE_TYPE_EVENT
    end
  end
end
