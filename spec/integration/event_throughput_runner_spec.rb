require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'dxlclient/service_registration_info'
require 'dxlclient/util'
require 'integration/client_helpers'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'event callbacks for multiple clients', :integration do
  it 'should be received for every event sent' do
    logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))
    logger.level = DXLClient::Logger::DEBUG

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

            connect_time_start = loop do
              connect_attempt_start = Time.now
              begin
                client.connect
                break connect_attempt_start
              rescue StandardError, MQTT::Exception => e
                retries -= 1
                has_failed = cross_client_mutex.synchronize do
                  all_clients_connect_retries += 1
                  if retries.zero? && !failed
                    logger.debugf('Failed to connect %d after retries: %s',
                                  thread_name, e.message)
                    failed = true
                    all_clients_connected_condition.broadcast
                  end
                  failed
                end
                break nil if has_failed || retries.zero?
              end
            end

            if connect_time_start
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

              cross_client_mutex.synchronize do
                clients_connected += 1
                if clients_connected == thread_count
                  logger.debug('All clients connected')
                  requests_start_time = Time.now
                  all_clients_connect_time = requests_start_time -
                                             connect_time_start
                  all_clients_connected_condition.broadcast

                  events_to_send.times do |count|
                    event = DXLClient::Message::Event.new(topic)
                    event.payload = count.to_s
                    begin
                      client.send_event(event)
                    rescue StandardError
                      failed = true
                      raise
                    end
                    if (count % 10).zero?
                      logger.debugf('Events sent: %d of %d.', count,
                                    events_to_send)
                    end
                  end
                else
                  if (clients_connected % 10).zero?
                    logger.debugf(
                      'Clients connected: %d of %d. Last connected: %s',
                      clients_connected, thread_count, thread_name
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
                  failed = true if clients_connected < thread_count
                end
              end

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
                if events_received == events_to_send
                  cross_client_mutex.synchronize do
                    threads_with_all_events_received += 1
                    logger.debugf(
                      'All events received for %s. %d of %d complete.',
                      thread_name,
                      threads_with_all_events_received,
                      thread_count
                    )
                  end
                else
                  logger.debugf(
                    'Timed out waiting for events for %s. Received %d of %d.',
                    thread_name, events_received, events_to_send
                  )
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
            events_received
          end
        end
      end

      start = Time.now

      events_received_per_thread = client_threads.collect do |thread|
        thread.join
        thread.value
      end

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
