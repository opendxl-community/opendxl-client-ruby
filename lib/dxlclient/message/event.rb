require 'dxlclient/message/message'

# Module under which all of the DXL client functionality resides.
module DXLClient
  module Message
    # {DXLClient::Message::Event} messages are sent using the
    # {DXLClient::Client#send_event} method of a client instance.
    class Event < DXLClient::Message::Message
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
end
