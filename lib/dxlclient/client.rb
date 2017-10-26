require 'mqtt'
require 'mqtt/client'

require 'dxlclient/callback_manager'
require 'dxlclient/dxl_error'
require 'dxlclient/message_encoder'
require 'dxlclient/mqtt_client'
require 'dxlclient/request_manager'
require 'dxlclient/response'
require 'dxlclient/service_manager'
require 'dxlclient/uuid_generator'

module DXLClient
  class Client
    REPLY_TO_PREFIX = '/mcafee/client/'.freeze
    DEFAULT_REQUEST_TIMEOUT = 60 * 60
    MQTT_QOS = 0

    private_constant :REPLY_TO_PREFIX, :DEFAULT_REQUEST_TIMEOUT,
                     :MQTT_QOS

    # @param config [DXLClient::Config]
    def initialize(config)
      @logger = DXLClient::Logger.logger(self.class)
      @client_id = UUIDGenerator.generate_id_as_string
      @reply_to_topic = "#{REPLY_TO_PREFIX}#{@client_id}"

      @mqtt_client = MQTTClient.new(config)
      @mqtt_client.on_message = method(:on_message)

      @callback_manager = CallbackManager.new(self)
      @request_manager = RequestManager.new(self, @reply_to_topic)
      @service_manager = ServiceManager.new(self)

      return unless block_given?
      begin
        yield(self)
      ensure
        destroy
      end
    end

    def connect
      @mqtt_client.connect
      @callback_manager.on_connect
    end

    def connected?
      @mqtt_client.connected?
    end

    def register_service_sync(service_reg_info, timeout)
      @service_manager.add_service_sync(service_reg_info, timeout)
    end

    def register_service_async(service_reg_info)
      @service_manager.add_service_async(service_reg_info)
    end

    def unregister_service_sync(service_reg_info, timeout)
      @service_manager.remove_service_sync(service_reg_info, timeout)
    end

    def unregister_service_async(service_reg_info)
      @service_manager.remove_service_async(service_reg_info)
    end

    def send_event(event)
      publish_message(event.destination_topic,
                      MessageEncoder.new.to_bytes(event))
    end

    def send_request(request)
      @logger.debugf('Sending request. Topic: %s. Id: %s.',
                     request.destination_topic, request.message_id)
      request.reply_to_topic = @reply_to_topic
      publish_message(request.destination_topic,
                      MessageEncoder.new.to_bytes(request))
    end

    def send_response(response)
      publish_message(response.destination_topic,
                      MessageEncoder.new.to_bytes(response))
    end

    def subscribe(topics)
      @logger.debug("Subscribing to topics: #{topics}.")
      @mqtt_client.subscribe(topics)
    end

    def async_request(request, response_callback = nil, &block)
      if response_callback && block_given?
        raise ArgumentError,
              'Only a callback or block (but not both) may be specified'
      end
      callback = block_given? ? block : response_callback
      @request_manager.async_request(request, callback)
    end

    def sync_request(request, timeout = DEFAULT_REQUEST_TIMEOUT)
      @request_manager.sync_request(request, timeout)
    end

    def unsubscribe(topics)
      @logger.debug("Unsubscribing from topics: #{topics}.")
      @mqtt_client.unsubscribe(topics)
    end

    def add_event_callback(topic, event_callback, subscribe_to_topic = true)
      @callback_manager.add_callback(DXLClient::Event, topic,
                                     event_callback,
                                     subscribe_to_topic)
    end

    def remove_event_callback(topic, event_callback)
      @callback_manager.remove_callback(DXLClient::Event, topic,
                                        event_callback)
    end

    def add_request_callback(topic, request_callback)
      @callback_manager.add_callback(DXLClient::Request, topic,
                                     request_callback)
    end

    def remove_request_callback(topic, request_callback)
      @callback_manager.remove_callback(DXLClient::Request, topic,
                                        request_callback)
    end

    def add_response_callback(topic, response_callback)
      @callback_manager.add_callback(DXLClient::Response, topic,
                                     response_callback)
    end

    def remove_response_callback(topic, response_callback)
      @callback_manager.remove_callback(DXLClient::Response, topic,
                                        response_callback)
    end

    def destroy
      @service_manager.destroy
      @request_manager.destroy
      @mqtt_client.disconnect
    rescue MQTT::NotConnectedException
      @logger.debug(
        'Unable to complete cleanup since MQTT client not connected'
      )
    end

    private

    def on_message(raw_message)
      message = MessageEncoder.new.from_bytes(raw_message.payload)
      message.destination_topic = raw_message.topic
      @callback_manager.on_message(message)
    end

    def publish_message(topic, payload)
      @mqtt_client.publish(topic, payload, false, MQTT_QOS)
    end
  end
end
