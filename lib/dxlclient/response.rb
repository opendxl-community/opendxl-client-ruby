require 'dxlclient/message'

module DXLClient
  class Response < Message
    attr_reader :request_message_id, :service_id

    def initialize()
      super('')
      @message_type = Message::MESSAGE_TYPE_RESPONSE
    end

    protected

    def unpack_message_v0(unpacker)
      super(unpacker)
      @request_message_id = unpacker.unpack()
      @service_id = unpacker.unpack()
    end
  end
end
