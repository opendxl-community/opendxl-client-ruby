require 'dxlclient'

$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__)))
require 'common'

SERVICE_TOPIC = '/isecg/sample/service'

logger = DXLClient::Logger.logger(File.basename(__FILE__, '.rb'))

begin
  logger.info("Service Provider - Load DXL config from: #{CONFIG_FILE}")
  config = DXLClient::Config.create_dxl_config_from_file(CONFIG_FILE)

  logger.info('Service Provider - Creating DXL Client')
  DXLClient::Client.new(config) do |client|
    logger.info('Service Provider - Connecting to Broker')
    client.connect

    class MyProviderCallback < DXLClient::RequestCallback
      def initialize(client, logger)
        @client = client
        @logger = logger
      end

      def on_request(request)
        @logger.infof("%s   Topic: %s\n   Request ID: %s\n   Payload: %s",
                      "Service Provider - Request received\n",
                      request.destination_topic,
                      request.message_id,
                      request.payload)

        @logger.infof('%s Creating Response for Request ID %s on %s',
                      'Service Provider -',
                      request.message_id,
                      request.destination_topic)
        response = DXLClient::Response.new(request)
        response.payload = 'Sample Response Payload'

        @logger.infof('%s Sending Response for Request ID %s on %s',
                      'Service Provider -',
                      request.message_id,
                      request.destination_topic)
        @client.send_response(response)
      end
    end

    info = DXLClient::ServiceRegistrationInfo.new(client,
                                                  '/mycompany/myservice')
    info.add_topic(SERVICE_TOPIC,
                   MyProviderCallback.new(client, logger))
    client.register_service_sync(info, 10)

    while true
      puts('   Enter 9 to quit')
      print('   Enter value: ')
      input = gets.chomp

      case input
        when '9'
          break
        else
          logger.info("Service Provider - Invalid input: #{input}")
      end
    end
  end
rescue => e
  logger.exception(e, 'Service Provider - Exception')
  exit(1)
end
