require 'dxlclient/message/message'

# Module under which all of the DXL client functionality resides.
module DXLClient
  module Message
    # {DXLClient::Message::Response} messages are sent by service instances
    # upon receiving {DXLClient::Message::Request} messages.
    class Response < DXLClient::Message::Message
      attr_reader :request_message_id, :service_id

      # rubocop: disable MethodLength

      # @param request [DXLClient::Message::Request]
      def initialize(request = nil)
        if request
          super(request.reply_to_topic)
          @request_message_id = request.message_id
          @service_id = request.service_id
          @client_ids = [request.source_client_id] if request.source_client_id
          @broker_ids = [request.source_broker_id] if request.source_broker_id
        else
          super('')
          @request_message_id = nil
          @service_id = ''
        end

        @request = request
        @message_type = DXLClient::Message::MESSAGE_TYPE_RESPONSE
      end
      # rubocop: enable MethodLength

      private

      def invoke_callback_class_instance(callback)
        callback.on_response(self)
      end

      def pack_message_v0(packer)
        super(packer)
        packer.write(@request_message_id)
        packer.write(@service_id)
      end

      def unpack_message_v0(unpacker)
        super(unpacker)
        @request_message_id = unpacker.unpack
        @service_id = unpacker.unpack
      end
    end
  end
end
