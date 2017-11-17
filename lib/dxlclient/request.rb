require 'dxlclient/message'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # {DXLClient::Request} messages are send using the
  # {DXLClient::Client#sync_request} and #{DXLClient::Client#async_request}
  # methods of a client instance.
  class Request < Message
    attr_accessor :reply_to_topic, :service_id

    def initialize(destination_topic = '')
      super(destination_topic)
      @reply_to_topic = nil
      @service_id = ''
      @message_type = Message::MESSAGE_TYPE_REQUEST
    end

    private

    def invoke_callback_class_instance(callback)
      callback.on_request(self)
    end

    def pack_message_v0(packer)
      super(packer)
      packer.write(@reply_to_topic)
      packer.write(@service_id)
    end

    def unpack_message_v0(unpacker)
      super(unpacker)
      @reply_to_topic = unpacker.unpack
      @service_id = unpacker.unpack
    end
  end
end
