require 'dxlclient/client'
require 'dxlclient/logger'
require 'dxlclient/message/event'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'dxlclient/service_registration_info'
require 'dxlclient/util'
require 'integration/client_helpers'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'event callbacks for multiple clients', :integration, :slow do
  it 'should be received for every event sent' do
    logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

    max_wait = 600
    max_connect_wait = 120
    max_connect_retries = 10
    events_to_send = 100
    thread_count = 1000
    expected_events_to_receive = events_to_send * thread_count

    start = Time.now

    cross_client_mutex = Mutex.new
    clients_connected = 0
    all_clients_connect_time = nil
    all_clients_connect_retries = 0
    requests_start_time = nil
    threads_with_all_events_received = 0
    failed = false

    all_clients_connected_condition = ConditionVariable.new

    ClientHelpers.with_integration_client(0) do |send_client|
      send_client.connect
      topic = "event_throughput_runner_spec_#{SecureRandom.uuid}"

      ClientHelpers.with_logged_messages_captured do
        # Create a DXL client for each thread up to thread count
        client_threads = Array.new(thread_count) do |counter|
          Thread.new do
            per_client_mutex = Mutex.new
            all_events_received_condition = ConditionVariable.new
            thread_name = DXLClient::Util.current_thread_name(
              "EventThroughputRunnerTest-#{counter + 1}"
            )

            events_received = 0

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
                # Add a callback to receive events
                client.add_event_callback(topic) do |event|
                  per_client_mutex.synchronize do
                    events_received += 1
                    if (events_received % 100).zero?
                      logger.debugf(
                        'Events received for %s: %d of %d. Last event: %s',
                        thread_name, events_received,
                        events_to_send, event.payload
                      )
                    end
                    if events_received == events_to_send
                      all_events_received_condition.broadcast
                    end
                  end
                end

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

                    # All clients have been connected so send events
                    events_to_send.times do |count|
                      event = DXLClient::Message::Event.new(topic)
                      event.payload = count.to_s
                      begin
                        send_client.send_event(event)
                      rescue StandardError => e
                        logger.errorf('Failed to send event: %s', e.message)
                        failed = true
                        raise
                      end
                      if (count % 10).zero?
                        logger.debugf('Events sent: %d of %d.', count,
                                      events_to_send)
                      end
                    end
                    logger.debug('All events sent')
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

                # Wait until all of the events have been received for the
                # client associated with the current thread
                per_client_mutex.synchronize do
                  ClientHelpers.while_not_done_and_time_remaining(
                    lambda do
                      !cross_client_mutex.synchronize { failed } &&
                      events_received < events_to_send
                    end,
                    max_wait,
                    start
                  ) do |wait_remaining|
                    all_events_received_condition.wait(per_client_mutex,
                                                       wait_remaining)
                  end
                  if events_received < events_to_send
                    logger.errorf(
                      'Timed out waiting for events for %s. Received %d of %d.',
                      thread_name, events_received, events_to_send
                    )
                  else
                    cross_client_mutex.synchronize do
                      threads_with_all_events_received += 1
                      logger.debugf(
                        'All events received for %s. %d of %d complete.',
                        thread_name,
                        threads_with_all_events_received,
                        thread_count
                      )
                    end
                  end
                end
              end
            end

            per_client_mutex.synchronize do
              logger.debugf(
                'Thread complete: %s. Events received: %d.',
                thread_name,
                events_received
              )
              # Return the number of events that the client received
              events_received
            end
          end
        end

        thread_error = nil
        # Collect the number of events received by each client (return value
        # from the associated thread)
        events_received_per_thread = client_threads.collect do |thread|
          begin
            thread.join
            thread.value
          rescue StandardError => e
            thread_error = e
            0
          end
        end

        expect(thread_error).to be_nil
        expect(events_received_per_thread).to all(eql(events_to_send))

        requests_time = Time.now - requests_start_time

        logger.debugf('Connect time: %f', all_clients_connect_time)
        logger.debugf('Connect retries: %d', all_clients_connect_retries)
        logger.debugf('Total events: %d', events_to_send)
        logger.debugf('Events/second: %f', events_to_send / requests_time)
        logger.debugf('Total events received: %d', expected_events_to_receive)
        logger.debugf('Total events received/second: %f',
                      expected_events_to_receive / requests_time)
        logger.debugf('Elapsed time: %f', requests_time)
      end
    end
  end
end
