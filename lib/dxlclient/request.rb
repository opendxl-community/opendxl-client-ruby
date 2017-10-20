require 'dxlclient/message'

module DXLClient
  class Request < Message
    attr_accessor :reply_to_topic, :service_id

    def initialize(destination_topic)
      super(destination_topic)
      @reply_to_topic = nil
      @service_id = ''
      @message_type = Message::MESSAGE_TYPE_REQUEST
    end

    private

    def pack_message(packer)
      super(packer)
      packer.write(@reply_to_topic)
      packer.write(@service_id)
    end
  end
end
