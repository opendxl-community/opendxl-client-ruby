require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/message/error_response'
require 'dxlclient/message/response'
require 'dxlclient/service_registration_info'
require 'integration/client_helpers'
require 'integration/test_service'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR
logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

describe 'async requests', :integration do
  it 'receive a response for every request made' do
    request_count = 100
    expected_response_count = request_count * 3
    max_wait = 60
    response_count = 0
    requests = {}

    ClientHelpers.with_integration_client do |client|
      client.connect

      test_service = TestService.new(client)
      topic = "async_request_spec_#{SecureRandom.uuid}"
      reg_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'async_request_spec_service'
      )
      reg_info.add_topic(topic, test_service)
      client.register_service_sync(reg_info, ClientHelpers::DEFAULT_TIMEOUT)

      response_mutex = Mutex.new
      response_condition = ConditionVariable.new

      response_callback = Proc.new do |response|
        response_mutex.synchronize do
          if requests[response.request_message_id]
            response_count += 1
            requests[response.request_message_id] += 1
            response_condition.broadcast
          end
        end
      end

      client.add_response_callback('', response_callback)

      request_count.times do
        request = DXLClient::Message::Request.new(topic)
        response_mutex.synchronize do
          requests[request.message_id] = 0
        end
        client.async_request(request)

        request = DXLClient::Message::Request.new(topic)
        response_mutex.synchronize do
          requests[request.message_id] = 0
        end
        client.async_request(request, response_callback)
      end

      start = Time.now
      response_mutex.synchronize do
        wait_remaining = max_wait
        while response_count < expected_response_count && wait_remaining > 0
          response_condition.wait(response_mutex, 1)
          wait_remaining = start - Time.now + max_wait
        end
      end
    end

    expect(response_count).to eql(expected_response_count)
    requests.values do |response_count|
      expect(response_count).to eql(3)
    end
  end
end
