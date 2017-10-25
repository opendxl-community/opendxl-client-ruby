require 'json'
require 'set'

require 'dxlclient/dxl_error'

module DXLClient
  class ServiceManager
    DXL_SERVICE_REGISTER_REQUEST_TOPIC = "/mcafee/service/dxl/svcregistry/register"
    DXL_SERVICE_UNREGISTER_REQUEST_TOPIC = "/mcafee/service/dxl/svcregistry/unregister"
    SERVICE_REGISTRATION_REQUEST_TIMEOUT = 10 # seconds
    SERVICE_UNREGISTRATION_REQUEST_TIMEOUT = 60 # seconds

    def initialize(client)
      @client = client
      @services = {}
    end

    def add_service_async(service_reg_info)
      request = add_service_common(service_reg_info)
      @client.async_request(request)
    end

    def add_service_sync(service_reg_info,
                         timeout=SERVICE_REGISTRATION_REQUEST_TIMEOUT)
      request = add_service_common(service_reg_info)
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::DXLError,
              "Error registering service: #{response.error_message} (#{response.error_code})"
      end

      @services[service_reg_info.service_id] = service_reg_info
    end

    def remove_service_async(service_reg_info)
      request = remove_service_common(service_reg_info)
      @client.async_request(request) do |response|
        @services.delete(service_reg_info.service_id)
      end
    end

    def remove_service_sync(service_reg_info,
                            timeout = SERVICE_UNREGISTRATION_REQUEST_TIMEOUT)
      request = remove_service_common(service_reg_info)
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::DXLError,
              "Error unregistering service: #{res.error_message} (#{res.error_code})"
      end

      @services.delete(service_reg_info.service_id)
    end

    def on_request(request)
      service_reg_info = @services[request.service_id]
      service_reg_info.callbacks(request.destination_topic).each do |callback|
        callback.on_request(request)
      end
    end

    def destroy
      @services.each_value do |service_reg_info|
        remove_service_sync(service_reg_info)
      end
    end

    private

    def add_service_common(service_reg_info)
      service_reg_info.topics.each do |topic|
        @client.subscribe(topic)
      end

      request = DXLClient::Request.new(DXL_SERVICE_REGISTER_REQUEST_TOPIC)
      request.payload = JSON.dump(serviceType: service_reg_info.service_type,
                                  metaData: service_reg_info.metadata,
                                  requestChannels: service_reg_info.topics,
                                  ttlMins: service_reg_info.ttl,
                                  serviceGuid: service_reg_info.service_id)
      request.destination_tenant_guids = service_reg_info.destination_tenant_guids
      request
    end

    def remove_service_common(service_reg_info)
      service_reg_info.topics.each do |topic|
        @client.unsubscribe(topic)
      end

      request = DXLClient::Request.new(DXL_SERVICE_UNREGISTER_REQUEST_TOPIC)
      request.payload = JSON.dump(serviceGuid: service_reg_info.service_id)
      request
    end
  end

  private_constant :ServiceManager
end
