require 'dxlclient'

config = {:host => '192.168.99.100',
          :port => 8883,
          :ca_file => '/home/jbarlow/documents/dxlcerts/ca-broker.crt',
          :client_cert_file => '/home/jbarlow/documents/dxlcerts/client.crt',
          :client_private_key_file => '/home/jbarlow/documents/dxlcerts/client.key'}

EVENT_TOPIC = '/isecg/sample/event'

puts('Event Subscriber - Creating DXL Client')
DXLClient::Client.new(config) do |client|
  puts('Event Subscriber - Connecting to Broker')
  client.connect

  class MyEventCallback < DXLClient::EventCallback
    def on_event(event)
      puts('Event Subscriber - Event received')
      puts("  Topic: #{event.destination_topic}")
      puts("  Payload: #{event.payload}")
    end
  end

  puts("Adding Event callback function to Topic: #{EVENT_TOPIC}")
  client.add_event_callback(EVENT_TOPIC, MyEventCallback.new)

  while true
    puts('   Enter 9 to quit')
    print('   Enter value: ')
    input = gets.chomp

    loop
      if input == '9'
        break
      puts("Event Publisher - Invalid input: #{input}")
    end
  end
end
