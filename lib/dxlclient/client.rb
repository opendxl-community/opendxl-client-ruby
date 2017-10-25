require 'mqtt'
require 'mqtt/client'

require 'dxlclient/dxl_error'
require 'dxlclient/event_manager'
require 'dxlclient/message_encoder'
require 'dxlclient/mqtt_client'
require 'dxlclient/request_manager'
require 'dxlclient/service_manager'
require 'dxlclient/uuid_generator'

module DXLClient
  class Client
    REPLY_TO_PREFIX = '/mcafee/client/'
    DEFAULT_REQUEST_TIMEOUT = 60 * 60
    MQTT_QOS = 0

    private_constant :REPLY_TO_PREFIX, :DEFAULT_REQUEST_TIMEOUT,
                     :MQTT_QOS

    # @param config [DXLClient::Config]
    def initialize(config)
      @logger = DXLClient::Logger.logger(self.class)
      @client_id = UUIDGenerator.generate_id_as_string

      @mqtt_client = MQTTClient.new(config)
      @mqtt_client.on_message = method(:on_message)

      @connected = false
      @event_manager = EventManager.new
      @request_manager = RequestManager.new(self)
      @service_manager = ServiceManager.new(self)
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
      @mqtt_client.connect
      @connected = true
      subscribe(@reply_to_topic)
    end

    def add_event_callback(topic, callback, subscribe_to_topic=true)
      @event_manager.add_event_callback(topic, callback)
      if subscribe_to_topic and !topic.nil?
        subscribe(topic)
      end
    end

    def register_service_sync(service_reg_info, timeout)
      @service_manager.add_service_sync(service_reg_info, timeout)
    end

    def unregister_service_sync(service_reg_info, timeout)
      @service_manager.remove_service_sync(service_reg_info, timeout)
    end

    def send_event(event)
      publish_message(event.destination_topic,
                      MessageEncoder.new.to_bytes(event))
    end

    def send_request(request)
      request.reply_to_topic = @reply_to_topic
      publish_message(request.destination_topic,
                      MessageEncoder.new.to_bytes(request))
    end

    def send_response(response)
      publish_message(response.destination_topic,
                      MessageEncoder.new.to_bytes(response))
    end

    def subscribe(topic)
      @mqtt_client.subscribe(topic)
    end

    def async_request(request, response_callback=nil)
      @request_manager.async_request(request, response_callback)
    end

    def sync_request(request, timeout=DEFAULT_REQUEST_TIMEOUT)
      @request_manager.sync_request(request, timeout)
    end

    def unsubscribe(topic)
      @mqtt_client.unsubscribe(topic)
    end

    def destroy
      if @connected
        @service_manager.destroy
        unsubscribe(@reply_to_topic)
        @mqtt_client.disconnect
      end
    end

    private

    def on_message(raw_message)
      message = MessageEncoder.new.from_bytes(raw_message.payload)
      message.destination_topic = raw_message.topic

      case message
        when DXLClient::Event
          @event_manager.on_event(message)
        when DXLClient::Request
          @service_manager.on_request(message)
        when DXLClient::Response
          @request_manager.on_response(message)
        else
          raise NotImplementedError,
                "No on_message implementation for #{message}"
      end
    end

    def publish_message(topic, payload)
      @mqtt_client.publish(topic, payload, false, MQTT_QOS)
    end
  end
end
