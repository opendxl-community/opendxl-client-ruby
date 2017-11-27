require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'dxlclient/service_registration_info'
require 'dxlclient/util'
require 'integration/client_helpers'
require 'integration/test_service'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'sync requests', :integration do
  it 'receive a response for every concurrent request made' do
    max_wait = 10
    request_count = 100
    ClientHelpers.with_integration_client(0) do |client|
      test_service = TestService.new(client)
      client.connect
      topic = 'sync_request_spec'
      reg_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'sync_request_spec_service'
      )
      reg_info.add_topic(topic, test_service)
      client.register_service_sync(reg_info, ClientHelpers::DEFAULT_TIMEOUT)

      request_threads = Array.new(request_count) do |counter|
        Thread.new do
          DXLClient::Util.current_thread_name("SyncRequestTest-#{counter + 1}")
          request = DXLClient::Message::Request.new(topic)
          response = client.sync_request(request, ClientHelpers::RESPONSE_WAIT)
          [request.message_id, response]
        end
      end

      start = Time.now
      responses = request_threads.collect do |thread|
        wait_remaining = start - Time.now + max_wait
        break if thread.join(wait_remaining).nil?
        thread.value
      end

      request_threads.each { |thread| thread.kill if thread.alive? }

      expect(responses).not_to be_nil, 'Timed out waiting for responses'
      expect(responses.count).to eql(request_count)
      responses.each do |request_message_id, response|
        expect(response).to be_an_instance_of(DXLClient::Message::Response)
        expect(response.request_message_id).to eql(request_message_id)
      end
    end
  end
end
