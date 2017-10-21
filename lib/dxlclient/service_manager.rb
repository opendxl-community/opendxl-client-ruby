require 'json'
require 'set'

require 'dxlclient/dxl_error'

module DXLClient
  class ServiceManager
    DXL_SERVICE_REGISTER_REQUEST_TOPIC = "/mcafee/service/dxl/svcregistry/register"
    SERVICE_REGISTRATION_REQUEST_TIMEOUT = 10 # seconds

    def initialize(client)
      @client = client
      @services = {}
    #
    #   @sync_wait_message_lock = Mutex.new
    #   @sync_wait_message_condition = ConditionVariable.new
    #   @sync_wait_message_ids = Set.new
    #   @sync_wait_message_responses = {}
    #
    #   @current_request_message_lock = Mutex.new
    #   @current_request_message_ids = Set.new
    end

    def add_service_sync(service_reg_info,
                         timeout=SERVICE_REGISTRATION_REQUEST_TIMEOUT)
      service_reg_info.topics.each do |topic|
        @client.subscribe(topic)
      end

      request = DXLClient::Request.new(DXL_SERVICE_REGISTER_REQUEST_TOPIC)
      request.payload = json_register_service(service_reg_info)
      request.destination_tenant_guids = service_reg_info.destination_tenant_guids
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::DXLError.new('Error registering service')
      end
      @services[service_reg_info.service_id] = service_reg_info
    end

    def on_request(request)
      service_reg_info = @services[request.service_id]
      service_reg_info.callbacks(request.destination_topic).each do |callback|
        callback.on_request(request)
      end
    end

    private

    def json_register_service(service_reg_info)
      JSON.dump(serviceType: service_reg_info.service_type,
                metaData: service_reg_info.metadata,
                requestChannels: service_reg_info.topics,
                ttlMins: service_reg_info.ttl,
                serviceGuid: service_reg_info.service_id)
    end
  end

  private_constant :ServiceManager
end
