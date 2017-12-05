require 'dxlclient/client'
require 'dxlclient/logger'
require 'dxlclient/message/request'
require 'dxlclient/service_registration_info'
require 'dxlclient/util'
require 'integration/client_helpers'
require 'integration/test_service'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'sync requests for multiple clients', :integration, :slow do
  it 'should be successful for every request made' do
    logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

    max_connect_wait = 120
    max_connect_retries = 10
    requests_to_send = 10
    thread_count = 1000
    expected_requests_to_process = requests_to_send * thread_count

    start = Time.now

    cross_client_mutex = Mutex.new
    clients_connected = 0
    all_clients_connect_time = nil
    all_clients_connect_retries = 0
    requests_start_time = nil
    threads_with_all_requests_processed = 0
    failed = false

    all_clients_connected_condition = ConditionVariable.new

    ClientHelpers.with_integration_client(0) do |server_client|
      test_service = TestService.new(server_client)
      server_client.connect
      topic = 'send_request_throughput_runner_spec'
      reg_info = DXLClient::ServiceRegistrationInfo.new(
        server_client, 'send_request_throughput_runner_spec_service'
      )
      reg_info.add_topic(topic, test_service)
      server_client.register_service_sync(reg_info,
                                          ClientHelpers::DEFAULT_TIMEOUT)

      ClientHelpers.with_logged_messages_captured do
        # Create a DXL client for each thread up to thread count
        client_threads = Array.new(thread_count) do |counter|
          Thread.new do
            request_times = nil
            thread_name = DXLClient::Util.current_thread_name(
              "SyncRequestThroughputRunnerTest-#{counter + 1}"
            )

            ClientHelpers.with_integration_client(0) do |client|
              retries = max_connect_retries

              # Attempt to connect client up to configured number of retries
              connect_time_start = loop do
                connect_attempt_start = Time.now
                begin
                  client.connect
                  break connect_attempt_start
                rescue StandardError => e
                  retries -= 1
                  has_failed = cross_client_mutex.synchronize do
                    all_clients_connect_retries += 1
                    if retries.zero? && !failed
                      ClientHelpers.disable_capture_logging
                      logger.errorf('Failed to connect %s after retries: %s',
                                    thread_name, e.message)
                      failed = true
                      all_clients_connected_condition.broadcast
                      raise
                    end
                    failed
                  end
                  break nil if has_failed || retries.zero?
                end
              end

              # Keep going if the client is connected...
              if connect_time_start
                # Pause the current thread until all clients from all threads
                # have been connected
                cross_client_mutex.synchronize do
                  clients_connected += 1
                  if clients_connected == thread_count
                    ClientHelpers.disable_capture_logging
                    logger.debug('All clients connected')
                    requests_start_time = Time.now
                    all_clients_connect_time = requests_start_time -
                                               connect_time_start
                    all_clients_connected_condition.broadcast
                  else
                    # Not all clients have been connected yet so wait.
                    if (clients_connected % 10).zero? &&
                       logger.level == DXLClient::Logger::DEBUG
                      puts(
                        format(
                          'Clients connected: %d of %d. Last connected: %s.',
                          clients_connected, thread_count, thread_name
                        )
                      )
                    end
                    ClientHelpers.while_not_done_and_time_remaining(
                      -> { !failed && clients_connected < thread_count },
                      max_connect_wait,
                      start
                    ) do |wait_remaining|
                      all_clients_connected_condition.wait(cross_client_mutex,
                                                           wait_remaining)
                    end
                    if !failed && clients_connected < thread_count
                      failed = true
                      all_clients_connected_condition.broadcast
                    end
                  end
                end

                # All clients have been connected so send requests
                request_times = Array.new(requests_to_send) do
                  request = DXLClient::Message::Request.new(topic)
                  start = Time.now
                  begin
                    response = client.sync_request(request)
                    end_time = Time.now - start
                    if response.is_a?(DXLClient::Message::ErrorResponse)
                      raise StandardError,
                            format(
                              'Received error payload for request: %s (%d)',
                              response.error_message, response.error_code
                            )
                    end
                    end_time
                  rescue StandardError => e
                    logger.errorf('Failed to send request: %s', e.message)
                    cross_client_mutex.synchronize { failed = true }
                    raise
                  end
                end

                cross_client_mutex.synchronize do
                  threads_with_all_requests_processed += 1
                  if (threads_with_all_requests_processed % 100).zero?
                    logger.debugf('All requests sent for %s, %d of %d complete',
                                  thread_name,
                                  threads_with_all_requests_processed,
                                  thread_count)
                  end
                end
              end
            end

            request_times
          end
        end

        thread_error = nil
        # Collect the times for requests sent by each client (return value
        # from the associated thread)
        request_times_per_thread = client_threads.collect do |thread|
          begin
            thread.join
            thread.value
          rescue StandardError => e
            thread_error = e
            []
          end
        end

        expect(thread_error).to be_nil

        request_counts_per_thread = request_times_per_thread.collect(&:length)

        expect(request_counts_per_thread).to all(eql(requests_to_send))

        requests_time = Time.now - requests_start_time
        sorted_request_times = request_times_per_thread.flatten.sort
        middle_request = expected_requests_to_process / 2
        response_median =
          if expected_requests_to_process.odd?
            sorted_request_times[middle_request]
          else
            (sorted_request_times[middle_request] +
              sorted_request_times[middle_request - 1]) / 2
          end

        logger.debugf('Connect time: %f', all_clients_connect_time)
        logger.debugf('Connect retries: %d', all_clients_connect_retries)
        logger.debugf('Request time: %f', requests_time)
        logger.debugf('Total requests: %d', expected_requests_to_process)
        logger.debugf('Requests/second: %f',
                      expected_requests_to_process / requests_time)
        logger.debugf('Average response time: %f',
                      sorted_request_times.reduce(:+) /
                        expected_requests_to_process)
        logger.debugf('Median response time: %f', response_median)
      end
    end
  end
end
