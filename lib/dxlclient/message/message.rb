require 'msgpack'

require 'dxlclient/uuid_generator'

# Module under which all of the DXL client functionality resides.
module DXLClient
  module Message
    # Base class for the different Data Exchange Layer (DXL) message types
    class Message
      DEFAULT_MESSAGE_VERSION = 2

      attr_accessor :broker_ids, :client_ids, :destination_topic,
                    :destination_tenant_guids, :other_fields, :payload,
                    :source_tenant_guid, :version

      protected :version=

      attr_reader :message_id, :message_type, :source_client_id,
                  :source_broker_id

      private_constant :DEFAULT_MESSAGE_VERSION

      # rubocop: disable MethodLength

      # Constructor
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
      # rubocop: enable MethodLength

      # Invokes the supplied callback with this message
      def invoke_callback(callback)
        return unless callback
        if callback.is_a?(Proc) || callback.is_a?(Method)
          callback.call(self)
        else
          invoke_callback_class_instance(callback)
        end
      end

      protected

      def unpack_message(unpacker)
        unpack_message_v0(unpacker)
        unpack_message_v1(unpacker) if @version > 0
        unpack_message_v2(unpacker) if @version > 1
      end

      def pack_message(packer)
        pack_message_v0(packer)
        pack_message_v1(packer)
        pack_message_v2(packer)
      end

      private

      def invoke_callback_class_instance(_callback)
        raise NotImplementedError,
              format('Callback support not available for this message type: %s',
                     self.class.name)
      end

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
        @message_id = unpacker.unpack
        @source_client_id = unpacker.unpack
        @source_broker_id = unpacker.unpack
        @broker_ids = unpacker.unpack
        @client_ids = unpacker.unpack
        @payload = unpacker.unpack
      end

      def unpack_message_v1(unpacker)
        @other_fields = Hash[*unpacker.unpack]
      end

      def unpack_message_v2(unpacker)
        @source_tenant_guid = unpacker.unpack
        @destination_tenant_guids = unpacker.unpack
      end
    end
  end
end
