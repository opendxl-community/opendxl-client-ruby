require 'dxlclient'
require 'thread'

config = {:host => '192.168.99.100',
          :port => 8883,
          :ca_file => '/home/jbarlow/documents/dxlcerts/ca-broker.crt',
          :client_cert_file => '/home/jbarlow/documents/dxlcerts/client.crt',
          :client_private_key_file => '/home/jbarlow/documents/dxlcerts/client.key'}

EVENT_TOPIC = '/isecg/sample/basicevent'

TOTAL_EVENTS = 1000

event_count_mutex = Mutex.new
event_count_condition = ConditionVariable.new
event_count = [0]

puts('Event Subscriber - Creating DXL Client')
DXLClient::Client.new(config) do |client|
  puts('Event Subscriber - Connecting to Broker')
  client.connect

  class MyEventCallback < DXLClient::EventCallback
    def initialize(event_count, event_count_condition, event_count_mutex)
      @event_count = event_count
      @event_count_condition = event_count_condition
      @event_count_mutex = event_count_mutex
    end

    def on_event(event)
      @event_count_mutex.synchronize do
        puts("Received event: #{event.payload}")
        @event_count[0] += 1
        @event_count_condition.broadcast
      end
    end
  end

  puts("Adding Event callback function to Topic: #{EVENT_TOPIC}")
  client.add_event_callback(EVENT_TOPIC,
                            MyEventCallback.new(event_count,
                                                event_count_condition,
                                                event_count_mutex))

  start = Time.now()

  TOTAL_EVENTS.times do |event_id|
    event = DXLClient::Event.new(EVENT_TOPIC)
    event.payload = event_id.to_s
    client.send_event(event)
  end

  puts('Waiting for events to be received...')
  event_count_mutex.synchronize do
    while event_count[0] < TOTAL_EVENTS do
      event_count_condition.wait(event_count_mutex)
    end
  end

  puts("Elapsed time (ms): #{(Time.now() - start) * 1000}")
end
