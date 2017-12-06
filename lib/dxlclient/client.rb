require 'dxlclient/callback_manager'
require 'dxlclient/connection_manager'
require 'dxlclient/error'
require 'dxlclient/message/event'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'dxlclient/message_encoder'
require 'dxlclient/mqtt_client_adapter'
require 'dxlclient/request_manager'
require 'dxlclient/service_manager'
require 'dxlclient/util'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # rubocop: disable ClassLength

  # Class responsible for all communication with the Data Exchange Layer (DXL)
  # fabric (it can be thought of as the "main" class).
  class Client
    REPLY_TO_PREFIX = '/mcafee/client/'.freeze
    DEFAULT_REQUEST_TIMEOUT = 60 * 60

    private_constant :REPLY_TO_PREFIX, :DEFAULT_REQUEST_TIMEOUT

    # rubocop: disable AbcSize, MethodLength

    # @param config [DXLClient::Config]
    def initialize(config, &block)
      @logger = DXLClient::Logger.logger(self.class.name)

      @reply_to_topic = "#{REPLY_TO_PREFIX}#{config.client_id}"

      @subscriptions = Set.new
      @subscription_lock = Mutex.new

      @mqtt_client = MQTTClientAdapter.new(config)
      @connection_manager = ConnectionManager.new(config,
                                                  @mqtt_client,
                                                  object_id)
      @callback_manager = create_callback_manager(config)
      @request_manager = RequestManager.new(self, @reply_to_topic)
      @service_manager = ServiceManager.new(self)

      initialize_mqtt_client
      handle_initialization_block(block)
    end

    # rubocop: enable AbcSize, MethodLength

    # Connect to a broker
    def connect
      @connection_manager.connect
    end

    def connected?
      @mqtt_client.connected?
    end

    # @return [DXLClient::Broker]
    def current_broker
      @connection_manager.current_broker
    end

    def disconnect
      @connection_manager.disconnect
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

    # @return [Set]
    def subscriptions
      @subscription_lock.synchronize do
        @subscriptions.clone
      end
    end

    def subscribe(topics)
      @subscription_lock.synchronize do
        topics_to_subscribe = send_subscribe_request(topics)
        if topics_to_subscribe.is_a?(String)
          @subscriptions.add(topics_to_subscribe)
        else
          @subscriptions.merge(topics_to_subscribe)
        end
      end
    end

    def unsubscribe(topics)
      @subscription_lock.synchronize do
        topics_to_unsubscribe = send_unsubscribe_request(topics)
        if topics_to_unsubscribe.respond_to?(:each)
          @subscriptions.subtract(topics_to_unsubscribe)
        else
          @subscriptions.delete(topics_to_unsubscribe)
        end
      end
    end

    def async_request(request, response_callback = nil, &block)
      @request_manager.async_request(
        request, callback_or_block(response_callback, block, true)
      )
    end

    def sync_request(request, timeout = DEFAULT_REQUEST_TIMEOUT)
      if @callback_manager.current_thread_in_callback_pool?
        raise DXLClient::Error::DXLError,
              format('%s %s %s',
                     'Synchronous requests may not be invoked while',
                     'handling an incoming message. The synchronous request',
                     'must be made on a different thread.')
      end
      @request_manager.sync_request(request, timeout)
    end

    def add_event_callback(topic, event_callback = nil,
                           subscribe_to_topic = true, &block)
      @callback_manager.add_callback(DXLClient::Message::Event, topic,
                                     callback_or_block(event_callback, block),
                                     subscribe_to_topic)
    end

    def remove_event_callback(topic, event_callback)
      @callback_manager.remove_callback(DXLClient::Message::Event, topic,
                                        event_callback)
    end

    def add_request_callback(topic, request_callback = nil,
                             subscribe_to_topic = false, &block)
      @callback_manager.add_callback(
        DXLClient::Message::Request, topic,
        callback_or_block(request_callback, block),
        subscribe_to_topic
      )
    end

    def remove_request_callback(topic, request_callback)
      @callback_manager.remove_callback(DXLClient::Message::Request, topic,
                                        request_callback)
    end

    def add_response_callback(topic, response_callback = nil,
                              subscribe_to_topic = false, &block)
      @callback_manager.add_callback(
        DXLClient::Message::Response, topic,
        callback_or_block(response_callback, block),
        subscribe_to_topic
      )
    end

    def remove_response_callback(topic, response_callback)
      @callback_manager.remove_callback(DXLClient::Message::Response, topic,
                                        response_callback)
    end

    def destroy
      @service_manager.destroy
      @request_manager.destroy
      unsubscribe_all
      @connection_manager.destroy
      @callback_manager.destroy
    rescue DXLClient::Error::IOError => e
      @logger.debugf(
        'Unable to complete cleanup since MQTT client not connected: %s',
        e.message
      )
    end

    private

    def callback_or_block(callback, block, allow_nil_for_both = false)
      if callback
        raise ArgumentError, 'Cannot specify callback and block' if block
        callback
      elsif block
        block
      elsif !allow_nil_for_both
        raise ArgumentError, 'Either a callback or block must be specified'
      end
    end

    def create_callback_manager(config)
      CallbackManager.new(
        self,
        config.incoming_message_queue_size,
        config.incoming_message_thread_pool_size
      )
    end

    def handle_initialization_block(block)
      return unless block
      begin
        block.call(self)
      ensure
        destroy
      end
    end

    def initialize_mqtt_client
      @connection_manager.add_connect_callback(method(:on_connect))
      @connection_manager.add_connect_callback(
        @service_manager.method(:on_connect)
      )
      @mqtt_client.add_message_callback(method(:on_message))
    end

    def topics_for_mqtt_client(topics)
      raise ArgumentError, 'topics cannot be a Hash' if topics.is_a?(Hash)
      [*topics]
    end

    def on_connect
      @subscription_lock.synchronize do
        unless @subscriptions.empty?
          topics = [*@subscriptions]
          @logger.debug("Resubscribing to topics: #{topics}.")
          @mqtt_client.subscribe(topics)
        end
      end
    end

    def on_message(raw_message)
      message = MessageEncoder.new.from_bytes(raw_message.payload)
      message.destination_topic = raw_message.topic
      @callback_manager.on_message(message)
    end

    def publish_message(topic, payload)
      @mqtt_client.publish(topic, payload)
    end

    def unsubscribe_all
      @subscription_lock.synchronize do
        send_unsubscribe_request(@subscriptions)
        @subscriptions.clear
      end
    end

    def send_subscribe_request(topics)
      topics_for_mqtt_client(topics).tap do |topics_to_subscribe|
        @logger.debug("Subscribing to topics: #{topics_to_subscribe}.")

        if @mqtt_client.connected?
          begin
            @mqtt_client.subscribe(topics_to_subscribe)
          rescue DXLClient::Error::IOError => e
            @logger.errorf(e.message)
          end
        end
      end
    end

    # rubocop: disable MethodLength

    def send_unsubscribe_request(topics)
      topics_for_mqtt_client(topics).tap do |topics_to_unsubscribe|
        unless topics_to_unsubscribe.empty?
          @logger.debugf('Unsubscribing from topics: %s',
                         topics_to_unsubscribe)
          if @mqtt_client.connected?
            begin
              @mqtt_client.unsubscribe(topics_to_unsubscribe)
            rescue DXLClient::Error::IOError => e
              @logger.errorf(e.message)
            end
          end
        end
      end
    end

    # rubocop: enable MethodLength
  end
end
