module DXLClient
  class EventManager
    def initialize
      @callbacks_by_topic = {}
    end

    def add_event_callback(topic, callback)
      callbacks = @callbacks_by_topic[topic]
      if !callbacks
        callbacks = Set.new
        @callbacks_by_topic[topic] = callbacks
      end
      callbacks.add(callback)
    end

    def on_event(event)
      @callbacks_by_topic[event.destination_topic].each do |callback|
        callback.on_event(event)
      end
    end
  end

  private_constant :EventManager
end
