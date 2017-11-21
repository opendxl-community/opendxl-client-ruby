require 'dxlclient'

$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__)))
require 'common'

SERVICE_TOPIC = '/isecg/sample/service'

logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

begin
  logger.info("Service Invoker - Load DXL config from: #{CONFIG_FILE}")
  config = DXLClient::Config.create_dxl_config_from_file(CONFIG_FILE)

  logger.info('Service Invoker - Creating DXL Client')
  DXLClient::Client.new(config) do |client|
    logger.info('Service Invoker - Connecting to Broker')
    client.connect

    loop do
      puts('   Enter 1 to send a Synchronous Event')
      puts('   Enter 2 to send an Asynchronous Event')
      puts('   Enter 9 to quit')
      print('   Enter value: ')
      input = gets.chomp

      case input
      when '1'
        logger.info(
          "Service Invoker - Creating Synchronous Request for topic #{SERVICE_TOPIC}"
        )
        request = DXLClient::Message::Request.new(SERVICE_TOPIC)
        request.payload =
          "Sample Synchronous Request Payload - Request ID: #{request.message_id}"

        logger.info(
          "Service Invoker - Sending Synchronous Request to #{SERVICE_TOPIC}"
        )
        response = client.sync_request(request)

        if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
          logger.infof("Service Invoker - %s:\n   Topic: %s\n   Payload: %s",
                       'Synchronous Error Response received',
                       response.destination_topic,
                       response.error_message)
        else
          logger.infof("Service Invoker - %s:\n   Topic: %s\n   Payload: %s",
                       'Synchronous Response received',
                       response.destination_topic,
                       response.payload)
        end
      when '2'
        logger.info(
          "Service Invoker - Creating Asynchronous Request for topic #{SERVICE_TOPIC}"
        )
        request = DXLClient::Message::Request.new(SERVICE_TOPIC)
        request.payload =
          "Sample Asynchronous Request Payload - Request ID: #{request.message_id}"

        logger.info(
          "Service Invoker - Sending Asynchronous Request to #{SERVICE_TOPIC}"
        )
        client.async_request(request) do |async_response|
          if async_response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
            logger.infof("%s:\n   Topic: %s\n   Request ID: %s\n   Error: %s",
                         'Service Invoker - Asynchronous Error Response received',
                         async_response.destination_topic,
                         async_response.request_message_id,
                         async_response.error_message)
          else
            logger.infof("%s:\n   Topic: %s\n   Request ID: %s\n   Payload: %s",
                         'Service Invoker - Asynchronous Response received',
                         async_response.destination_topic,
                         async_response.request_message_id,
                         async_response.payload)
          end
        end
      when '9'
        break
      else
        logger.info("Service Invoker - Invalid input: #{input}")
      end
    end
  end
rescue StandardError => e
  logger.exception(e, 'Service Invoker - Exception')
  exit(1)
end
