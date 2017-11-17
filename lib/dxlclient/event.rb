require 'dxlclient/message'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # {DXLClient::Event} messages are sent using the
  # {DXLClient::Client#send_event} method of a client instance.
  class Event < Message
    attr_reader :message_type

    def initialize(destination_topic = '')
      super(destination_topic)
      @message_type = DXLClient::Message::MESSAGE_TYPE_EVENT
    end

    private

    def invoke_callback_class_instance(callback)
      callback.on_event(self)
    end
  end
end
