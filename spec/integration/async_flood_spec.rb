require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/message/error_response'
require 'dxlclient/message/response'
require 'dxlclient/service_registration_info'
require 'integration/client_helpers'
require 'integration/test_service'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR
logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

describe 'a flood of async requests', :integration do
  it 'receive a response for every request made' do
    request_count = 1000
    max_wait = 90
    topic = "async_flood_sync_#{SecureRandom.uuid}"
    error_count = 0
    response_count = 0

    ClientHelpers.with_integration_client do |client|
      reg_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'async_flood_sync_service'
      )
      client.connect

      reg_info.add_topic(topic) do |request|
        sleep(0.05)
        response = DXLClient::Message::Response.new(request)
        response.payload = request.payload
        client.send_response(response)
      end
      client.register_service_sync(reg_info, 10)

      response_mutex = Mutex.new
      response_condition = ConditionVariable.new

      ClientHelpers.with_integration_client do |client2|
        client2.connect

        client2.add_response_callback('') do |response|
          response_mutex.synchronize do
            if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
              logger.errorf('Received error response: %s',
                            response.error_message)
              error_count += 1
              response_condition.broadcast
            else
              response_count += 1
              if (response_count % 10).zero?
                logger.debugf('Received response %d', response_count)
              end
              response_condition.broadcast if response_count == request_count
            end
          end
        end

        request_count.times do |count|
          request = DXLClient::Message::Request.new(topic)
          request.payload = count.to_s
          client2.async_request(request)
          current_error_count = response_mutex.synchronize { error_count }
          break unless current_error_count.zero?
        end

        start = Time.now
        response_mutex.synchronize do
          wait_remaining = max_wait
          while response_count < request_count && error_count.zero? &&
                wait_remaining > 0
            response_condition.wait(response_mutex, wait_remaining)
            wait_remaining = start - Time.now + max_wait
          end

          # Terminate the client connections to end the test early if
          # at least one request has failed
          unless error_count.zero?
            client.disconnect
            client2.disconnect
          end
        end
      end
    end

    expect(error_count).to eql(0)
    expect(response_count).to eql(request_count)
  end
end
