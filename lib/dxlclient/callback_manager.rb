require 'dxlclient/callback_info'
require 'dxlclient/queue_thread_pool'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Manager which handles registrations and dispatches messages to callbacks.
  class CallbackManager
    # @param client [DXLClient::Client]
    def initialize(client, callback_queue_size, callback_thread_pool_size)
      @logger = DXLClient::Logger.logger(self.class.name)
      @client = client
      @callbacks_by_class = {}
      @callback_thread_pool = QueueThreadPool.new(
        callback_queue_size,
        callback_thread_pool_size,
        "DXLMessageCallbacks-#{@client.object_id}"
      )
    end

    def add_callback(klass, topic, callback, subscribe_to_topic = true)
      topic = '' if topic.nil?
      @client.subscribe(topic) if subscribe_to_topic && !topic.empty?
      callbacks_by_topic = callbacks_for_class!(klass)
      callbacks = callbacks_for_topic!(callbacks_by_topic, topic)
      callbacks.add(CallbackInfo.new(callback, subscribe_to_topic))
    end

    def remove_callback(klass, topic, callback, unsubscribe = true)
      topic = '' if topic.nil?
      callbacks_by_topic = @callbacks_by_class[klass]
      return unless callbacks_by_topic
      callbacks = callbacks_by_topic[topic]
      return unless callbacks
      entry = callbacks.find do |callback_info|
        callback_info.callback == callback
      end
      unsubscribe_callback(callbacks, entry, topic, unsubscribe)
    end

    # @param message [DXLClient::Message::Message]
    def on_message(message)
      @logger.debugf('Received message. Type: %s. Id: %s.',
                     message.class.name, message.message_id)
      matching_callbacks = get_matching_callbacks(message)
      invoke_callbacks_for_class!(matching_callbacks, message)
    end

    def destroy
      @callback_thread_pool.destroy
    end

    private

    def callbacks_for_message(message)
      @callbacks_by_class[message.class] ||
        @callbacks_by_class[
          @callbacks_by_class.keys.find do |klass|
            message.is_a?(klass)
          end] || {}
    end

    def callbacks_for_class!(klass)
      callbacks_by_topic = @callbacks_by_class[klass]
      unless callbacks_by_topic
        callbacks_by_topic = {}
        @callbacks_by_class[klass] = callbacks_by_topic
      end
      callbacks_by_topic
    end

    def callbacks_for_topic!(callbacks_by_topic, topic)
      callbacks = callbacks_by_topic[topic]
      unless callbacks
        callbacks = Set.new
        callbacks_by_topic[topic] = callbacks
      end
      callbacks
    end

    def get_matching_callbacks(message)
      callbacks_for_message(message).select do |topic|
        topic.empty? ||
          (topic == message.destination_topic) ||
          (topic[-1] == '#' &&
            message.destination_topic.start_with?(topic[0...-1]))
      end
    end

    def invoke_callbacks_for_class!(callbacks, message)
      if callbacks.length.zero?
        @logger.debugf('No callbacks registered for topic: %s. Id: %s.',
                       message.destination_topic, message.message_id)
      else
        callbacks.each_value do |topic_callbacks|
          invoke_callbacks_for_topic!(topic_callbacks, message)
        end
      end
    end

    def invoke_callbacks_for_topic!(callbacks, message)
      callbacks.map(&:callback).each do |callback|
        @callback_thread_pool.add_task do
          message.invoke_callback(callback)
        end
      end
    end

    def topic_subscribed?(topic)
      @callbacks_by_class.values.any? do |callbacks_by_topic|
        callbacks_by_topic[topic] &&
          callbacks_by_topic[topic].any?(&:subscribe?)
      end
    end

    def unsubscribe_callback(callbacks, callback, topic, unsubscribe)
      return if !callbacks.delete?(callback) || !unsubscribe ||
                topic_subscribed?(topic) || topic.empty?
      @client.unsubscribe(topic)
    end
  end

  private_constant :CallbackManager
end
