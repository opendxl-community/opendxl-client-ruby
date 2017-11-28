require 'dxlclient/client'
require 'dxlclient/message/event'
require 'integration/client_helpers'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'event callbacks', :integration do
  it 'received for every event request made' do
    send_count = 10_000
    max_wait = 120
    receive_count = 0
    events = Set.new

    ClientHelpers.with_integration_client(0) do |client|
      client.connect

      topic = "event_request_spec_#{SecureRandom.uuid}"
      event_mutex = Mutex.new
      all_events_received_condition = ConditionVariable.new

      client.add_event_callback(topic) do |event|
        event_mutex.synchronize do
          if events.include?(event.message_id)
            events.delete(event.message_id)
            receive_count += 1
            if receive_count == send_count
              all_events_received_condition.broadcast
            end
          end
        end
      end

      send_count.times do
        event = DXLClient::Message::Event.new(topic)
        event_mutex.synchronize do
          events.add(event.message_id)
        end
        client.send_event(event)
      end

      start = Time.now
      event_mutex.synchronize do
        wait_remaining = max_wait
        while receive_count < send_count && wait_remaining > 0
          all_events_received_condition.wait(event_mutex, wait_remaining)
          wait_remaining = start - Time.now + max_wait
        end

        expect(receive_count).to eql(send_count)
        expect(events.size).to eql(0)
      end
    end
  end
end
