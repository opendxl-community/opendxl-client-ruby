require 'dxlclient/callback_info'

module DXLClient
  class CallbackManager
    # @param client [DXLClient::Client]
    def initialize(client)
      @logger = DXLClient::Logger.logger(self.class.name)
      @client = client
      @callbacks_by_class = {}
    end

    # @param callback_info [DXLClient::CallbackInfo]
    def add_callback(klass, topic, callback, subscribe_to_topic=true)
      @client.subscribe(topic) if subscribe_to_topic

      callbacks_by_topic = @callbacks_by_class[klass]
      unless callbacks_by_topic
        callbacks_by_topic = {}
        @callbacks_by_class[klass] = callbacks_by_topic
      end

      callbacks = callbacks_by_topic[topic]
      unless callbacks
        callbacks = Set.new
        callbacks_by_topic[topic] = callbacks
      end

      callbacks.add(CallbackInfo.new(callback, subscribe_to_topic))
    end

    # @param callback_info [DXLClient::CallbackInfo]
    def remove_callback(klass, topic, callback, unsubscribe=true)
      callbacks_by_topic = @callbacks_by_class[klass]
      if callbacks_by_topic
        callbacks = callbacks_by_topic[topic]
        if callbacks
          entry = callbacks.find do |callback_info|
            callback_info.callback == callback
          end
          if callbacks.delete?(entry) && unsubscribe &&
              !topic_subscribed?(topic)
            @client.unsubscribe(topic)
          end
        end
      end
    end

    # @param message [DXLClient::Message]
    def on_message(message)
      @logger.debugf('Received message. Type: %s. Id: %s.',
                     message.class.name, message.message_id)
      class_callbacks = @callbacks_by_class[message.class] ||
          @callbacks_by_class[
              @callbacks_by_class.keys.find do |klass|
                message.is_a?(klass)
              end]

      if class_callbacks
        topic_callbacks = class_callbacks[message.destination_topic]
        if topic_callbacks
          topic_callbacks.map(&:callback).each do |callback|
            message.invoke_callback(callback)
          end
        else
          @logger.debugf('No callbacks registered for topic: %s. Id: %s.',
                         message.destination_topic, message.message_id)
        end
      else
        @logger.debugf('No callbacks registered for message type: %s. Id: %s.',
                       message.class.name, message.message_id)
      end
    end

    private

    def topics_to_subscribe
      @callbacks_by_class.values.reduce(Set.new) do |topics, callbacks_by_topic|
        topics_for_klass = callbacks_by_topic.reduce(Set.new) do |klass_topics, (topic, callbacks)|
          if callbacks.any?(&:subscribe?)
            klass_topics.add(topic)
          else
            klass_topics
          end
        end
        topics.merge(topics_for_klass)
      end
    end

    def topic_subscribed?(topic)
      @callbacks_by_class.values.any? do |callbacks_by_topic|
        callbacks_by_topic[topic] &&
            callbacks_by_topic[topic].any?(&:subscribe?)
      end
    end
  end

  private_constant :CallbackManager
end
