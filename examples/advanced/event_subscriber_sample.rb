require 'dxlclient'

config = {:host => '192.168.99.100',
          :port => 8883,
          :ca_file => '/home/jbarlow/documents/dxlcerts/ca-broker.crt',
          :client_cert_file => '/home/jbarlow/documents/dxlcerts/client.crt',
          :client_private_key_file => '/home/jbarlow/documents/dxlcerts/client.key'}

DXLClient::Client.new(config) do |client|
  puts('Connecting...')
  client.connect
  puts('Subscribe and listen...')
  client.add_event_callback('/isecg/sample/event')
end
