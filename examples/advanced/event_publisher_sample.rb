require 'dxlclient'

config = {:host => '192.168.99.100',
          :port => 8883,
          :ca_file => '/home/jbarlow/documents/dxlcerts/ca-broker.crt',
          :client_cert_file => '/home/jbarlow/documents/dxlcerts/client.crt',
          :client_private_key_file => '/home/jbarlow/documents/dxlcerts/client.key'}

EVENT_TOPIC = '/isecg/sample/event'

DxlClient::Client.new(config) do |client|
  puts('Connecting...')
  client.connect

  while true
    puts('   Enter 1 to publish a DXL Event')
    puts('   Enter 9 to quit')
    print('   Enter value: ')
    input = gets.chomp

    case input
      when '1'
        puts("Event Publisher - Creating Event for Topic #{EVENT_TOPIC}")
        event = DxlClient::Event.new(EVENT_TOPIC)
        event.payload = 'Sample Event Payload'
        puts("Event Publisher - Publishing Event to #{EVENT_TOPIC}")
        client.send_event(event)
      when '9'
        break
      else
        puts("Event Publisher - Invalid input: #{input}")
    end
  end
end
