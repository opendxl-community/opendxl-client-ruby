require 'json'
require 'set'

require 'dxlclient/dxl_error'

module DXLClient
  class ServiceManager
    DXL_SERVICE_REGISTER_REQUEST_TOPIC = "/mcafee/service/dxl/svcregistry/register"
    DXL_SERVICE_UNREGISTER_REQUEST_TOPIC = "/mcafee/service/dxl/svcregistry/unregister"
    SERVICE_REGISTRATION_REQUEST_TIMEOUT = 10 # seconds
    SERVICE_UNREGISTRATION_REQUEST_TIMEOUT = 60 # seconds

    # @param client [DXLClient::Client]
    def initialize(client)
      @client = client
      @services = {}
    end

    def add_service_async(service_reg_info)
      request = register_service_request(service_reg_info)
      @client.async_request(request) do |response|
        add_service_callbacks(service_reg_info)
      end
      @services[service_reg_info.service_id] = service_reg_info
    end

    def add_service_sync(service_reg_info,
                         timeout=SERVICE_REGISTRATION_REQUEST_TIMEOUT)
      request = register_service_request(service_reg_info)
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::DXLError,
              "Error registering service: #{response.error_message} (#{response.error_code})"
      end
      add_service_callbacks(service_reg_info)
      @services[service_reg_info.service_id] = service_reg_info
    end

    def remove_service_async(service_reg_info)
      request = unregister_service_request(service_reg_info)
      @client.async_request(request) do |response|
        remove_service_callbacks(service_reg_info)
      end
      @services.delete(service_reg_info.service_id)
    end

    def remove_service_sync(service_reg_info,
                            timeout = SERVICE_UNREGISTRATION_REQUEST_TIMEOUT)
      request = unregister_service_request(service_reg_info)
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::DXLError,
              "Error unregistering service: #{res.error_message} (#{res.error_code})"
      end
      remove_service_callbacks(service_reg_info)
      @services.delete(service_reg_info.service_id)
    end

    def destroy
      @services.each_value do |service_reg_info|
        remove_service_sync(service_reg_info)
      end
    end

    private

    def register_service_request(service_reg_info)
      request = DXLClient::Request.new(DXL_SERVICE_REGISTER_REQUEST_TOPIC)
      request.payload = JSON.dump(serviceType: service_reg_info.service_type,
                                  metaData: service_reg_info.metadata,
                                  requestChannels: service_reg_info.topics,
                                  ttlMins: service_reg_info.ttl,
                                  serviceGuid: service_reg_info.service_id)
      request.destination_tenant_guids = service_reg_info.destination_tenant_guids
      request
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_callbacks(service_reg_info)
      service_reg_info.topics.each do |topic|
        service_reg_info.callbacks(topic).each do |callback|
          @client.add_request_callback(topic, callback)
        end
      end
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def unregister_service_request(service_reg_info)
      request = DXLClient::Request.new(DXL_SERVICE_UNREGISTER_REQUEST_TOPIC)
      request.payload = JSON.dump(serviceGuid: service_reg_info.service_id)
      request
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def remove_service_callbacks(service_reg_info)
      service_reg_info.topics.each do |topic|
        service_reg_info.callbacks(topic).each do |callback|
          @client.remove_request_callback(topic, callback)
        end
      end
    end
  end

  private_constant :ServiceManager
end
