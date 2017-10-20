require 'dxlclient/message'

module DXLClient
  class ErrorResponse < Response
    attr_reader :error_code, :error_message

    def initialize(error_code=0, error_message="")
      super()
      @error_code = error_code
      @error_message = error_message
      @message_type = DXLClient::Message::MESSAGE_TYPE_ERROR
    end

    private

    def unpack_message_v0(unpacker)
      super(unpacker)
      @error_code = unpacker.unpack()
      @error_message = unpacker.unpack()
    end
  end
end
