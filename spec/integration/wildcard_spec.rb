require 'dxlclient/client'
require 'dxlclient/logger'
require 'dxlclient/message/event'
require 'dxlclient/util'
require 'integration/client_helpers'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'callbacks subscribed with wildcard topics', :integration do
  logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

  it 'should be invoked for a matching event' do
    max_wait = 10
    event_mutex = Mutex.new
    event_received_condition = ConditionVariable.new
    events = []

    ClientHelpers.with_integration_client(0) do |client|
      client.connect

      topic = 'wildcard_event_spec'
      event_payload = 'Unit test payload'

      client.add_event_callback("#{topic}/#") do |event|
        event_mutex.synchronize do
          events.push(event)
          event_received_condition.broadcast
        end
      end

      event = DXLClient::Message::Event.new("#{topic}/foo")
      event.payload = event_payload
      client.send_event(event)

      event_mutex.synchronize do
        ClientHelpers.while_not_done_and_time_remaining(
          -> { events.length.zero? }, max_wait
        ) do |wait_remaining|
          event_received_condition.wait(event_mutex, wait_remaining)
        end
      end

      expect(events.length).to eql(1)
      expect(events[0].payload).to eql(event_payload)
    end
  end

  it 'should be invoked for a large number of matching events', :slow do
    def measure_performance(client, with_wildcard, topic_exists,
                            sub_count, query_multiplier, logger)
      max_wait = 5000
      expected_message_id_count = sub_count * query_multiplier
      expected_event_count = expected_message_id_count *
                             (with_wildcard && topic_exists ? 2 : 1)

      event_mutex = Mutex.new
      all_events_received_condition = ConditionVariable.new
      event_count = 0
      event_message_ids = Set.new

      event_payload = DXLClient::Util.generate_id_as_string
      topic_prefix = "/wildcard_performance_spec/#{event_payload}/"

      client.add_event_callback('#', nil, false) do |event|
        if event.payload == event_payload
          event_mutex.synchronize do
            event_count += 1
            event_message_ids.add(event.message_id)
            if (event_count % sub_count).zero?
              logger.debugf('Messages received: %d of %d',
                            event_count,
                            expected_event_count)
            end
            if event_count == expected_event_count
              all_events_received_condition.broadcast
            end
          end
        end
      end

      client.subscribe("#{topic_prefix}#") if with_wildcard

      sub_count.times do |count|
        client.subscribe("#{topic_prefix}#{count}")
        if (count % 1000).zero?
          logger.debugf('Subscribed: %d of %d', count, sub_count)
        end
      end
      logger.debug('Subscriptions complete')

      start = Time.now

      expected_message_id_count.times do |count|
        topic_suffix = (count % sub_count) + (topic_exists ? 0 : sub_count)
        event = DXLClient::Message::Event.new("#{topic_prefix}#{topic_suffix}")
        event.payload = event_payload
        client.send_event(event)
      end

      event_mutex.synchronize do
        ClientHelpers.while_not_done_and_time_remaining(
          -> { event_count < expected_event_count }, max_wait
        ) do |wait_remaining|
          all_events_received_condition.wait(event_mutex, wait_remaining)
        end
      end

      [Time.now - start,
       expected_message_id_count, event_message_ids.length,
       expected_event_count, event_count]
    end

    sub_count = 10_000
    query_multiplier = 10

    ClientHelpers.with_integration_client(0) do |client|
      client.connect

      with_wildcard_event_time, with_wildcard_expected_message_id_count,
      with_wildcard_message_id_count, with_wildcard_expected_event_count,
      with_wildcard_event_count = measure_performance(
        client, true, false, sub_count, query_multiplier, logger
      )
      expect(with_wildcard_expected_message_id_count)
        .to eq(with_wildcard_message_id_count)
      expect(with_wildcard_expected_event_count)
        .to eq(with_wildcard_event_count)

      without_wildcard_event_time, without_wildcard_expected_message_id_count,
      without_wildcard_message_id_count, without_wildcard_expected_event_count,
      without_wildcard_event_count = measure_performance(
        client, false, true, sub_count, query_multiplier, logger
      )
      expect(without_wildcard_expected_message_id_count)
        .to eq(without_wildcard_message_id_count)
      expect(without_wildcard_expected_event_count)
        .to eq(without_wildcard_event_count)

      with_wildcard_topic_exists_event_time,
      with_wildcard_topic_exists_expected_message_id_count,
      with_wildcard_topic_exists_message_id_count,
      with_wildcard_topic_exists_expected_event_count,
      with_wildcard_topic_exists_event_count = measure_performance(
        client, true, true, sub_count, query_multiplier, logger
      )

      expect(with_wildcard_topic_exists_expected_message_id_count)
        .to eq(with_wildcard_topic_exists_message_id_count)
      expect(with_wildcard_topic_exists_expected_event_count)
        .to eq(with_wildcard_topic_exists_event_count)

      logger.debugf('With wildcard: %f', with_wildcard_event_time)
      logger.debugf('Without wildcard: %f', without_wildcard_event_time)
      logger.debugf('With wildcard topic exists: %f',
                    with_wildcard_topic_exists_event_time)

      expect(with_wildcard_event_time)
        .to be < (2 * without_wildcard_event_time)
    end
  end
end
