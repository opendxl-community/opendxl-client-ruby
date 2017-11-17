require 'msgpack'

require 'dxlclient/dxl_error'
require 'dxlclient/error_response'
require 'dxlclient/event'
require 'dxlclient/message'
require 'dxlclient/response'
require 'dxlclient/uuid_generator'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Class which decodes a message encodes in the DXL wire format into a
  # {DXLClient::Message} object and vice-versa. This class is defined as
  # a subclass of {DXLClient::Message} so that it has access to protected
  # methods for doing encoding and decoding operations.
  class MessageEncoder < Message
    def initialize; end

    def from_bytes(raw)
      io = StringIO.new(raw)
      unpacker = MessagePack::Unpacker.new(io)
      version = unpacker.unpack
      message_type = unpacker.unpack

      case message_type
      when DXLClient::Message::MESSAGE_TYPE_EVENT
        message = DXLClient::Event.new('')
      when DXLClient::Message::MESSAGE_TYPE_REQUEST
        message = DXLClient::Request.new('')
      when DXLClient::Message::MESSAGE_TYPE_RESPONSE
        message = DXLClient::Response.new
      when DXLClient::Message::MESSAGE_TYPE_ERROR
        message = DXLClient::ErrorResponse.new
      else
        raise DXLClient::DXLError,
              "Unknown message type: #{message_type}"
      end

      message.version = version
      message.unpack_message(unpacker)
      message
    end

    def to_bytes(message)
      unless message.message_type
        raise NotImplementedError, 'Unknown message type'
      end

      io = StringIO.new
      packer = MessagePack::Packer.new(io)
      packer.write(message.version)
      packer.write(message.message_type)
      message.pack_message(packer)
      packer.flush
      io.string
    end
  end

  private_constant :MessageEncoder
end
