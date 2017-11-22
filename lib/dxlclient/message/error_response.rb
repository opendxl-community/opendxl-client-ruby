require 'dxlclient/message/message'
require 'dxlclient/message/response'

# Module under which all of the DXL client functionality resides.
module DXLClient
  module Message
    # {DXLClient::Message::ErrorResponse} messages are sent by the DXL fabric
    # itself or service instances upon receiving {DXLClient::Message::Request}
    # messages.
    class ErrorResponse < DXLClient::Message::Response
      attr_reader :error_code, :error_message

      def initialize(request = nil, error_code = 0, error_message = '')
        super(request)
        @error_code = error_code
        @error_message = error_message
        @message_type = DXLClient::Message::MESSAGE_TYPE_ERROR
      end

      private

      def pack_message_v0(packer)
        super(packer)
        packer.write(@error_code)
        packer.write(@error_message)
      end

      def unpack_message_v0(unpacker)
        super(unpacker)
        @error_code = unpacker.unpack
        @error_message = unpacker.unpack
      end
    end
  end
end
