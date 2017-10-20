require 'msgpack'

require 'dxlclient/message'
require 'dxlclient/uuid_generator'

module DXLClient
  class MessageUnpacker < Message
    def initialize
    end

    def from_bytes(raw)
      io = StringIO.new(raw)
      unpacker = MessagePack::Unpacker.new(io)
      version = unpacker.unpack()
      message_type = unpacker.unpack()

      case message_type
        when DXLClient::Message::MESSAGE_TYPE_RESPONSE
          message = DXLClient::Response.new
        when DXLClient::Message::MESSAGE_TYPE_ERROR
          message = DXLClient::ErrorResponse.new
        else
          raise DXLClient::DXLError.new("Unknown message type: #{message_type}")
      end

      message.version = version
      if message.version > 0
        message.unpack_message(unpacker)
      end
      message
    end
  end

  private_constant :MessageUnpacker
end


