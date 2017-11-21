require 'thread'
require 'dxlclient'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__)))
require 'common'

EVENT_TOPIC = '/isecg/sample/basicevent'

TOTAL_EVENTS = 1000

event_count_mutex = Mutex.new
event_count_condition = ConditionVariable.new
event_count = [0]

config = DXLClient::Config.create_dxl_config_from_file(CONFIG_FILE)
config.incoming_message_thread_pool_size = 10

DXLClient::Client.new(config) do |client|
  client.connect

  class MyEventCallback < DXLClient::Callback::EventCallback
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
