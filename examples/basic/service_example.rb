require 'securerandom'
require 'dxlclient'

puts("pwd: #{Dir.pwd}")

$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__)))
require 'common'

SERVICE_TOPIC = "/isecg/sample/mybasicservice/#{SecureRandom.uuid}"

config = DXLClient::Config.create_dxl_config_from_file(CONFIG_FILE)

DXLClient::Client.new(config) do |client|
  client.connect

  class MyRequestCallback < DXLClient::RequestCallback
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

  info = DXLClient::ServiceRegistrationInfo.new(client, "myService")
  info.add_topic(SERVICE_TOPIC, MyRequestCallback.new(client))
  client.register_service_sync(info, 10)

  req = DXLClient::Request.new(SERVICE_TOPIC)
  req.payload = 'ping'
  res = client.sync_request(req, 5)
  if res.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
    raise Exception.new("Error: #{res.error_message} (#{res.error_code})")
  else
    puts("Client received response payload: #{res.payload}")
  end
end
