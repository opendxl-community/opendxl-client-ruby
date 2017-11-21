require 'msgpack'

require 'dxlclient/error'
require 'dxlclient/message'
require 'dxlclient/message/error_response'
require 'dxlclient/message/event'
require 'dxlclient/message/message'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'dxlclient/uuid_generator'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Class which decodes a message encodes in the DXL wire format into a
  # {DXLClient::Message::Message} object and vice-versa. This class is defined
  # as a subclass of {DXLClient::Message::Message} so that it has access to
  # protected methods for doing encoding and decoding operations.
  class MessageEncoder < DXLClient::Message::Message
    MESSAGE_TYPE_TO_CLASS = {
      DXLClient::Message::MESSAGE_TYPE_EVENT => DXLClient::Message::Event,
      DXLClient::Message::MESSAGE_TYPE_REQUEST => DXLClient::Message::Request,
      DXLClient::Message::MESSAGE_TYPE_RESPONSE => DXLClient::Message::Response,
      DXLClient::Message::MESSAGE_TYPE_ERROR =>
        DXLClient::Message::ErrorResponse
    }.freeze

    private_constant :MESSAGE_TYPE_TO_CLASS

    def initialize; end

    def from_bytes(raw)
      io = StringIO.new(raw)
      unpacker = MessagePack::Unpacker.new(io)
      version = unpacker.unpack
      message_type = unpacker.unpack

      message_class = MESSAGE_TYPE_TO_CLASS[message_type]
      unless message_class
        raise DXLClient::Error::DXLError, "Unknown message type: #{message_type}"
      end
      create_message(message_class, unpacker, version)
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

    private

    def create_message(message_class, unpacker, version)
      message_class.new.tap do |message|
        message.version = version
        message.unpack_message(unpacker)
      end
    end
  end

  private_constant :MessageEncoder
end
