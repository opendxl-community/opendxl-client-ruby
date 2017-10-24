
module DXLClient
  class ServiceRegistrationInfo
    attr_accessor :destination_tenant_guids, :metadata, :ttl
    attr_reader :service_id, :service_type

    DEFAULT_TTL = 60 # minutes

    def initialize(client, service_type)
      @client = client
      @service_type = service_type

      @callbacks_by_topic = {}
      @destination_tenant_guids = []
      @metadata = {}
      @service_id = UUIDGenerator.generate_id_as_string
      @ttl = DEFAULT_TTL
    end

    def add_topic(topic, callback)
      callbacks = @callbacks_by_topic[topic]
      unless callbacks
        callbacks = Set.new
        @callbacks_by_topic[topic] = callbacks
      end
      callbacks.add(callback)
    end

    def topics
      @callbacks_by_topic.keys
    end

    def callbacks(topic)
      @callbacks_by_topic[topic]
    end
  end
end
