require 'dxlclient/callback_info'
require 'dxlclient/queue_thread_pool'

module DXLClient
  class CallbackManager
    # @param client [DXLClient::Client]
    def initialize(client, callback_queue_size, callback_thread_pool_size)
      @logger = DXLClient::Logger.logger(self.class.name)
      @client = client
      @callbacks_by_class = {}
      @callback_thread_pool = QueueThreadPool.new(callback_queue_size,
                                                  callback_thread_pool_size,
                                                  'DXLMessageCallbacks')
    end

    def add_callback(klass, topic, callback, subscribe_to_topic = true)
      if topic.nil? || topic.length.zero?
        raise ArgumentError, 'topic cannot be empty'
      end

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

    def remove_callback(klass, topic, callback, unsubscribe = true)
      callbacks_by_topic = @callbacks_by_class[klass]
      return unless callbacks_by_topic
      callbacks = callbacks_by_topic[topic]
      return unless callbacks
      entry = callbacks.find do |callback_info|
        callback_info.callback == callback
      end
      if callbacks.delete?(entry) && unsubscribe &&
         !topic_subscribed?(topic)
        @client.unsubscribe(topic)
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
        matching_class_callbacks = class_callbacks.select do |topic|
          (topic == message.destination_topic) ||
            (topic[-1] == '#' &&
              message.destination_topic.start_with?(topic[0...-1]))
        end
        if matching_class_callbacks.length.zero?
          @logger.debugf('No callbacks registered for topic: %s. Id: %s.',
                         message.destination_topic, message.message_id)
        else
          matching_class_callbacks.each_value do |topic_callbacks|
            topic_callbacks.map(&:callback).each do |callback|
              @callback_thread_pool.add_task do
                message.invoke_callback(callback)
              end
            end
          end
        end
      else
        @logger.debugf('No callbacks registered for message type: %s. Id: %s.',
                       message.class.name, message.message_id)
      end
    end

    private

    def topics_to_subscribe
      @callbacks_by_class.values.reduce(Set.new) do |topics, callbacks_by_topic|
        topics_for_klass =
          callbacks_by_topic.reduce(
            Set.new
          ) do |klass_topics, (topic, callbacks)|
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
