require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'dxlclient/service_registration_info'
require 'integration/client_helpers'
require 'integration/test_service'

MAX_WAIT = 10
REQUEST_COUNT = 100

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'sync requests', :integration do
  it 'receive a response for every concurrent request made' do
    ClientHelpers.with_integration_client(0) do |client|
      test_service = TestService.new(client)
      client.connect
      topic = 'event_testing'
      reg_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'sync_request_runner_service'
      )
      reg_info.add_topic(topic, test_service)
      client.register_service_sync(reg_info, ClientHelpers::DEFAULT_TIMEOUT)

      request_threads = Array.new(REQUEST_COUNT) do |counter|
        Thread.new do
          Thread.current.name = "SyncRequestTest-#{counter + 1}"
          request = DXLClient::Message::Request.new(topic)
          response = client.sync_request(request, ClientHelpers::RESPONSE_WAIT)
          [request.message_id, response]
        end
      end

      start = Time.now

      responses = request_threads.collect do |thread|
        max_time_to_wait = Time.now - start + MAX_WAIT
        break if thread.join(max_time_to_wait).nil?
        thread.value
      end

      request_threads.each { |thread| thread.kill if thread.alive? }

      expect(responses).not_to be_nil, 'Timed out waiting for responses'
      expect(responses.count).to eql(REQUEST_COUNT)
      responses.each do |request_message_id, response|
        expect(response).to be_an_instance_of(DXLClient::Message::Response)
        expect(response.request_message_id).to eql(request_message_id)
      end
    end
  end
end
