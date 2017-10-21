require 'dxlclient/message'

module DXLClient
  class Response < Message
    attr_reader :request_message_id, :service_id

    # @param request [DXLClient::Request]
    def initialize(request = nil)
      if request
        super(request.reply_to_topic)
        @request_message_id = request.message_id
        @service_id = request.service_id
        if request.source_client_id
          @client_ids = [request.source_client_id]
        end
        if request.source_broker_id
          @broker_ids = [request.source_broker_id]
        end
      else
        super('')
        @request_message_id = nil
        @service_id = ''
      end

      @request = request
      @message_type = Message::MESSAGE_TYPE_RESPONSE
    end

    protected

    def pack_message_v0(packer)
      super(packer)
      packer.write(@request_message_id)
      packer.write(@service_id)
    end

    def unpack_message_v0(unpacker)
      super(unpacker)
      @request_message_id = unpacker.unpack()
      @service_id = unpacker.unpack()
    end
  end
end
