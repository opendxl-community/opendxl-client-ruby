require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/service_registration_info'
require 'integration/client_helpers'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'message payload in an async request', :integration do
  it 'should be delivered with the expected content' do
    max_wait = 5

    payload_received_mutex = Mutex.new
    payload_received_condition = ConditionVariable.new
    payload_received = nil

    expected_string = 'SslUtils'
    expected_byte = 0b1
    expected_int = 123_456

    ClientHelpers.with_integration_client do |client|
      client.connect

      topic = "message_payload_spec_#{SecureRandom.uuid}"
      reg_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'message_payload_spec_service'
      )
      reg_info.add_topic(topic) do |request|
        payload_received_mutex.synchronize do
          payload_received = request.payload
          payload_received_condition.broadcast
        end
      end

      client.register_service_sync(reg_info, ClientHelpers::DEFAULT_TIMEOUT)

      request = DXLClient::Message::Request.new(topic)

      send_io = StringIO.new
      packer = MessagePack::Packer.new(send_io)
      packer.write(expected_string)
      packer.write(expected_byte)
      packer.write(expected_int)
      packer.flush
      request.payload = send_io.string

      client.async_request(request)

      payload_received_mutex.synchronize do
        ClientHelpers.while_not_done_and_time_remaining(
          -> { payload_received.nil? },
          max_wait
        ) do |wait_remaining|
          payload_received_condition.wait(payload_received_mutex,
                                          wait_remaining)
        end
      end

      expect(payload_received).to_not be_nil

      receive_io = StringIO.new(request.payload)
      unpacker = MessagePack::Unpacker.new(receive_io)

      expect(unpacker.unpack).to eql(expected_string)
      expect(unpacker.unpack).to eql(expected_byte)
      expect(unpacker.unpack).to eql(expected_int)
    end
  end
end
