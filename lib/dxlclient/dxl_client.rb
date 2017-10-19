require 'mqtt'
require 'mqtt/client'

require 'dxlclient/uuid_generator'

module DxlClient
  class Client
    REPLY_TO_PREFIX = '/mcafee/client/'
    DEFAULT_WAIT = 60 * 60
    DEFAULT_QOS = 0

    private_constant :REPLY_TO_PREFIX, :DEFAULT_WAIT

    def initialize(config)
      @client_id = DxlClient::UuidGenerator.generate_id_as_string

      client = MQTT::Client.new(:client_id => @client_id)
      client.host = config[:host]
      client.port = config[:port]
      client.cert_file = config[:client_cert_file]
      client.key_file = config[:client_private_key_file]
      client.ca_file = config[:ca_file]
      client.ssl = true
      @client = client

      @reply_to_topic = "#{REPLY_TO_PREFIX}#{@client_id}"

      if block_given?
        begin
          yield(self)
        ensure
          destroy
        end
      end
    end

    def connect
      @client.connect
    end

    def add_event_callback(topic)
      printf("Subscribing to %s\n", topic)
      @client.subscribe(topic)
      @client.get do |callback_topic, message|
        printf("Got topic=%s, message=%s\n", callback_topic, message)
      end
    end

    def send_event(event)
      publish_message(event.destination_topic, event.to_bytes)
    end

    def send_request(request)
      request.reply_to_topic = @reply_to_topic
      publish_message(request.destination_topic, request.to_bytes)
    end

    def sync_request(request, timeout=DEFAULT_WAIT)
      send_request(request)
    end

    def destroy
      if @client
        @client.disconnect
      end
    end

    private
    def publish_message(topic, payload)
      @client.publish(topic, payload, false, DEFAULT_QOS)
    end
  end
end
