require 'dxlclient'

config = {:host => '192.168.99.100',
          :port => 8883,
          :ca_file => '/home/jbarlow/documents/dxlcerts/ca-broker.crt',
          :client_cert_file => '/home/jbarlow/documents/dxlcerts/client.crt',
          :client_private_key_file => '/home/jbarlow/documents/dxlcerts/client.key'}

SERVICE_TOPIC = '/isecg/sample/basicservice'

DxlClient::Client.new(config) do |client|
  puts('Connecting...')
  client.connect

  req = DxlClient::Request.new(SERVICE_TOPIC)
  req.payload = 'ping'
  puts('Subscribe and listen...')
  res = client.sync_request(req)
  if res.message_type != Message.MESSAGE_TYPE_ERROR
    puts("Client received response payload: #{res.payload}")
  end
end
