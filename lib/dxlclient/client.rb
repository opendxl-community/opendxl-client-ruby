require 'mqtt'
require 'mqtt/client'

require 'dxlclient/message_unpacker'
require 'dxlclient/mqtt_client'
require 'dxlclient/request_manager'
require 'dxlclient/uuid_generator'

module DXLClient
  class Client
    REPLY_TO_PREFIX = '/mcafee/client/'
    DEFAULT_WAIT = 60 * 60
    MQTT_QOS = 0
    MQTT_VERSION = '3.1.1'

    private_constant :REPLY_TO_PREFIX, :DEFAULT_WAIT,
                     :MQTT_QOS, :MQTT_VERSION

    def initialize(config)
      @client_id = UuidGenerator.generate_id_as_string

      client = MQTTClient.new(
          :host => config[:host],
          :port => config[:port],
          :client_id => @client_id,
          :version => MQTT_VERSION,
          :clean_session => true,
          :ssl => true)

      client.cert_file = config[:client_cert_file]
      client.key_file = config[:client_private_key_file]
      client.ca_file = config[:ca_file]
      client.on_message = method(:on_message)
      @client = client

      @request_manager = RequestManager.new(self)
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
      subscribe(@reply_to_topic)
    end

    def add_event_callback(topic)
      printf("Subscribing to %s\n", topic)
      subscribe(topic)
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

    def subscribe(topic)
      @client.subscribe(topic)
    end

    def sync_request(request, timeout=DEFAULT_WAIT)
      @request_manager.sync_request(request, timeout)
    end

    def destroy
      if @client
        @client.disconnect
      end
    end

    private

    def on_message(raw_message)
      message = MessageUnpacker.new.from_bytes(raw_message.payload)
      message.destination_topic = raw_message.topic
      @request_manager.on_response(message)
    end

    def publish_message(topic, payload)
      @client.publish(topic, payload, false, MQTT_QOS)
    end
  end
end
