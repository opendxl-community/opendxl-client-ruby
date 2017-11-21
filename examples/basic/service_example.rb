require 'dxlclient'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__)))
require 'common'

SERVICE_TOPIC = "/isecg/sample/mybasicservice"

config = DXLClient::Config.create_dxl_config_from_file(CONFIG_FILE)

DXLClient::Client.new(config) do |client|
  client.connect

  class MyRequestCallback < DXLClient::Callback::RequestCallback
    def initialize(client)
      @client = client
    end

    def on_request(request)
      puts("Service received request payload: #{request.payload}")
      response = DXLClient::Response.new(request)
      response.payload = 'pong'
      @client.send_response(response)
    end
  end

  info = DXLClient::ServiceRegistrationInfo.new(client, 'myService')
  info.add_topic(SERVICE_TOPIC, MyRequestCallback.new(client))
  client.register_service_sync(info, 10)

  request = DXLClient::Request.new(SERVICE_TOPIC)
  request.payload = 'ping'
  response = client.sync_request(request)
  if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
    raise Exception, "Error: #{response.error_message} (#{response.error_code})"
  else
    puts("Client received response payload: #{response.payload}")
  end
end
