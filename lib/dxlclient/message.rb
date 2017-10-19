require 'msgpack'

require 'dxlclient/uuid_generator'

module DxlClient
  class Message
    MESSAGE_VERSION = 2

    MESSAGE_TYPE_REQUEST = 0
    MESSAGE_TYPE_RESPONSE = 1
    MESSAGE_TYPE_EVENT = 2
    MESSAGE_TYPE_ERROR = 3

    attr_accessor :destination_topic, :payload
    # attr_reader :broker_ids, :client_ids
    attr_reader :message_id, :message_type, :version,
                :source_client_id, :source_broker_id

    def initialize(destination_topic)
      @destination_topic = destination_topic

      # Version 0 fields
      @version = MESSAGE_VERSION
      @message_type = nil
      @message_id = DxlClient::UuidGenerator.generate_id_as_string
      @source_client_id = ''
      @source_broker_id = ''
      @broker_ids = []
      @client_ids = []

      # Version 1 fields
      @other_fields = {}

      # Version 2 fields
      @source_tenant_guid = ''
      @destination_tenant_guids = []

      # @source_client_id = 'myclient'
      # @source_broker_id = 'mybroker'
      # @broker_ids = [ 'brokerid1', 'brokerid2']
      # @client_ids = [ 'clientid1', 'clientid2']
      # @other_fields = { 'field1': 'val1', 'field2': 'val2'}
      # @source_tenant_guid = 'stguid1'
      # @destination_tenant_guids = [ 'dtguid1', 'dtguid2']
    end

    def to_bytes
      if not @message_type
        raise NotImplementedError('Unknown message type')
      end

      io = StringIO.new
      packer = MessagePack::Packer.new(io)
      packer.write(@version)
      packer.write(@message_type)
      pack_message(packer)
      pack_message_v1(packer)
      pack_message_v2(packer)
      packer.flush
      io.string
    end

    private
    def pack_message(packer)
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
  end
end
