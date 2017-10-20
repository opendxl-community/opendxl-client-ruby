require 'dxlclient'

config = {:host => '192.168.99.100',
          :port => 8883,
          :ca_file => '/home/jbarlow/documents/dxlcerts/ca-broker.crt',
          :client_cert_file => '/home/jbarlow/documents/dxlcerts/client.crt',
          :client_private_key_file => '/home/jbarlow/documents/dxlcerts/client.key'}

SERVICE_TOPIC = '/isecg/sample/basicservice'

DXLClient::Client.new(config) do |client|
  puts('Connecting...')
  client.connect

  req = DXLClient::Request.new(SERVICE_TOPIC)
  req.payload = 'ping'
  puts('Subscribe and listen...')
  res = client.sync_request(req, 5)
  if res.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
    raise Exception.new("Error: #{res.error_message} (#{res.error_code})")
  else
    puts("Client received response payload: #{res.payload}")
  end
end
