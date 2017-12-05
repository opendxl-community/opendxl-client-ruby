require 'dxlclient/client'
require 'dxlclient/message/error_response'
require 'dxlclient/message/request'
require 'dxlclient/service_registration_info'
require 'integration/client_helpers'
require 'integration/test_service'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe DXLClient::Client do
  it 'should connect and disconnect to a broker without error' do
    ClientHelpers.with_integration_client(0) do |client|
      client.connect
      expect(client.connected?).to be true
      client.disconnect
      expect(client.connected?).to be false
    end
  end

  it 'should subscribe and unsubscribe to a topic without error' do
    ClientHelpers.with_integration_client(0) do |client|
      client.connect
      topic = "client_spec_subscribe_#{SecureRandom.uuid}"
      client.subscribe(topic)
      expect(client.subscriptions).to include(topic)
      client.unsubscribe(topic)
      expect(client.subscriptions).to_not include(topic)
    end
  end

  it 'should properly receive an error response from a service' do
    ClientHelpers.with_integration_client(0) do |client|
      test_service = TestService.new(client)
      client.connect

      error_code = 9090
      error_message = 'My error message'
      topic = "client_spec_error_message_#{SecureRandom.uuid}"

      reg_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'client_spec_error_message_service'
      )
      reg_info.add_topic(topic, test_service)
      client.register_service_sync(reg_info, ClientHelpers::DEFAULT_TIMEOUT)

      test_service.return_error = true
      test_service.error_code = error_code
      test_service.error_message = error_message

      response = client.sync_request(DXLClient::Message::Request.new(topic))
      expect(response).to be_an_instance_of(DXLClient::Message::ErrorResponse)
      expect(response.error_code).to eql(error_code)
      expect(response.error_message).to eql(error_message)
    end
  end

  it 'should receive event callbacks on all threads in callback pool' do
    max_wait = 10
    thread_count = 10

    threads_receiving_callbacks = Set.new
    event_mutex = Mutex.new
    new_callback_thread_condition = ConditionVariable.new
    all_callback_threads_exercised_condition = ConditionVariable.new

    topic = "client_spec_event_callback_threads_#{SecureRandom.uuid}"

    start = Time.now

    ClientHelpers.with_integration_client(0, thread_count) do |client|
      client.connect
      client.add_event_callback(topic) do
        event_mutex.synchronize do
          unless threads_receiving_callbacks.include?(Thread.current)
            threads_receiving_callbacks.add(Thread.current)
            new_callback_thread_condition.broadcast
            if threads_receiving_callbacks.size == thread_count
              all_callback_threads_exercised_condition.broadcast
            else
              ClientHelpers.while_not_done_and_time_remaining(
                -> { threads_receiving_callbacks.size < thread_count },
                max_wait,
                start
              ) do |wait_remaining|
                all_callback_threads_exercised_condition.wait(
                  event_mutex, wait_remaining
                )
              end
            end
          end
        end
      end

      event_mutex.synchronize do
        ClientHelpers.while_not_done_and_time_remaining(
          -> { threads_receiving_callbacks.size < thread_count }, max_wait
        ) do |wait_remaining|
          event = DXLClient::Message::Event.new(topic)
          client.send_event(event)
          new_callback_thread_condition.wait(event_mutex, wait_remaining)
        end

        # Terminate the client connection to end the test quickly if not
        # all events were received
        unless threads_receiving_callbacks.size == thread_count
          client.disconnect
        end

        expect(threads_receiving_callbacks.size).to eql(thread_count)
      end
    end
  end

  it 'should raise an error for a sync request attempted from a callback' do
    max_wait = 5
    event_topic = \
      "client_spec_sync_from_callback_event_#{SecureRandom.uuid}"
    request_topic = \
      "client_spec_sync_from_callback_request_#{SecureRandom.uuid}"

    ClientHelpers.with_integration_client do |client|
      sync_request_mutex = Mutex.new
      sync_request_result_condition = ConditionVariable.new
      sync_request_result = nil
      sync_request_received = nil

      client.connect

      reg_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'client_spec_sync_request_service'
      )
      reg_info.add_topic(request_topic) do |request|
        sync_request_received = request
        client.send_response(DXLClient::Message::Response.new(request))
      end
      client.register_service_sync(reg_info, ClientHelpers::DEFAULT_TIMEOUT)

      client.add_event_callback(event_topic) do
        sync_request_mutex.synchronize do
          begin
            request = DXLClient::Message::Request.new(request_topic)
            sync_request_result = client.sync_request(request)
          rescue StandardError => e
            sync_request_result = e
          end
          sync_request_result_condition.broadcast
        end
      end

      client.send_event(DXLClient::Message::Event.new(event_topic))

      sync_request_mutex.synchronize do
        ClientHelpers.while_not_done_and_time_remaining(
          -> { sync_request_result.nil? }, max_wait
        ) do |wait_remaining|
          sync_request_result_condition.wait(sync_request_mutex,
                                             wait_remaining)
        end
      end

      expect(sync_request_result)
        .to be_an_instance_of(DXLClient::Error::DXLError)
      expect(sync_request_result.message)
        .to include('must be made on a different thread')
      expect(sync_request_received).to be_nil
    end
  end
end
