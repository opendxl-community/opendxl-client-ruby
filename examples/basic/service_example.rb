require 'securerandom'

require 'dxlclient'

config = {:host => '192.168.99.100',
          :port => 8883,
          :ca_file => '/home/jbarlow/documents/dxlcerts/ca-broker.crt',
          :client_cert_file => '/home/jbarlow/documents/dxlcerts/client.crt',
          :client_private_key_file => '/home/jbarlow/documents/dxlcerts/client.key'}

SERVICE_TOPIC = "/isecg/sample/mybasicservice/#{SecureRandom.uuid}"

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
