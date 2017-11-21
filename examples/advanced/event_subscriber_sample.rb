require 'dxlclient'

$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__)))
require 'common'

EVENT_TOPIC = '/isecg/sample/event'

logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

begin
  logger.info("Event Subscriber - Load DXL config from: #{CONFIG_FILE}")
  config = DXLClient::Config.create_dxl_config_from_file(CONFIG_FILE)

  logger.info('Event Subscriber - Creating DXL Client')
  DXLClient::Client.new(config) do |client|
    logger.info('Event Subscriber - Connecting to Broker')
    client.connect

    class MySubscriberCallback < DXLClient::Callback::EventCallback
      def initialize(logger)
        @logger = logger
      end

      def on_event(event)
        @logger.infof("Event Subscriber - %s\n   Topic: %s\n   Payload: %s",
                      'Event received', event.destination_topic, event.payload)
      end
    end

    logger.info("Adding Event callback function to Topic: #{EVENT_TOPIC}")
    client.add_event_callback(EVENT_TOPIC, MySubscriberCallback.new(logger))

    loop do
      puts('   Enter 9 to quit')
      print('   Enter value: ')
      input = gets.chomp

      break if input == '9'
      logger.info("Event Subscriber - Invalid input: #{input}")
    end
  end
rescue StandardError => e
  logger.exception(e, 'Event Subscriber - Exception')
  exit(1)
end
