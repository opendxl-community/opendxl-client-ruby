require 'msgpack'

require 'dxlclient/uuid_generator'

module DXLClient
  class Message
    DEFAULT_MESSAGE_VERSION = 2

    MESSAGE_TYPE_REQUEST = 0
    MESSAGE_TYPE_RESPONSE = 1
    MESSAGE_TYPE_EVENT = 2
    MESSAGE_TYPE_ERROR = 3

    attr_accessor :broker_ids, :client_ids, :destination_topic,
                  :destination_tenant_guids, :other_fields, :payload,
                  :source_tenant_guid

    attr_reader :message_id, :message_type, :version,
                :source_client_id, :source_broker_id

    def initialize(destination_topic)
      @destination_topic = destination_topic

      # Version 0 fields
      @version = DEFAULT_MESSAGE_VERSION
      @message_type = nil
      @message_id = UUIDGenerator.generate_id_as_string

      @source_client_id = ''
      @source_broker_id = ''
      @broker_ids = []
      @client_ids = []

      # Version 1 fields
      @other_fields = {}

      # Version 2 fields
      @source_tenant_guid = ''
      @destination_tenant_guids = []
    end

    protected

    def version=(version)
      @version = version
    end

    def unpack_message(unpacker)
      unpack_message_v0(unpacker)
      if @version > 0
        unpack_message_v1(unpacker)
      end
      if @version > 1
        unpack_message_v2(unpacker)
      end
    end

    def pack_message(packer)
      pack_message_v0(packer)
      pack_message_v1(packer)
      pack_message_v2(packer)
    end

    private

    def pack_message_v0(packer)
      packer.write(@message_id)
      packer.write(@source_client_id)
      packer.write(@source_broker_id)
      packer.write(@broker_ids)
      packer.write(@client_ids)
      packer.write(@payload)
    end

    def pack_message_v1(packer)
      packer.write(@other_fields.flatten)
    end

    def pack_message_v2(packer)
      packer.write(@source_tenant_guid)
      packer.write(@destination_tenant_guids)
    end

    def unpack_message_v0(unpacker)
      @message_id = unpacker.unpack()
      @source_client_id = unpacker.unpack()
      @source_broker_id = unpacker.unpack()
      @broker_ids = unpacker.unpack()
      @client_ids = unpacker.unpack()
      @payload = unpacker.unpack()
    end

    def unpack_message_v1(unpacker)
      @other_fields = Hash[*unpacker.unpack()]
    end

    def unpack_message_v2(unpacker)
      @source_tenant_guid = unpacker.unpack()
      @destination_tenant_guids = unpacker.unpack()
    end
  end
end
