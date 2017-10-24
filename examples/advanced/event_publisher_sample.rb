require 'dxlclient'

$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__)))
require 'common'

EVENT_TOPIC = '/isecg/sample/event'

logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

begin
  logger.info("Event Publisher - Load DXL config from: #{CONFIG_FILE}")
  config = DXLClient::Config.create_dxl_config_from_file(CONFIG_FILE)

  logger.info('Event Publisher - Creating DXL Client')
  DXLClient::Client.new(config) do |client|
    logger.info('Event Publisher - Connecting to Broker')
    client.connect

    while true
      puts('   Enter 1 to publish a DXL Event')
      puts('   Enter 9 to quit')
      print('   Enter value: ')
      input = gets.chomp

      case input
        when '1'
          logger.info(
              "Event Publisher - Creating Event for Topic #{EVENT_TOPIC}")
          event = DXLClient::Event.new(EVENT_TOPIC)
          event.payload = 'Sample Event Payload'
          logger.info("Event Publisher - Publishing Event to #{EVENT_TOPIC}")
          client.send_event(event)
        when '9'
          break
        else
          logger.info("Event Publisher - Invalid input: #{input}")
      end
    end
  end
rescue => e
  logger.exception(e,"Event Publisher - Exception")
  exit(1)
end
