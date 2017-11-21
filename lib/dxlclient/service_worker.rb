require 'json'
require 'set'

require 'dxlclient/dxl_error'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # rubocop:disable ClassLength

  # Worker thread logic which re-registers locally registered services with the
  # DXL fabric as the service's Time-To-Live value expires and as disconnect /
  # reconnect events occur.
  class ServiceWorker
    DXL_SERVICE_REGISTER_REQUEST_TOPIC = \
      '/mcafee/service/dxl/svcregistry/register'.freeze

    # @param service_manager [DXLClient::ServiceManager]
    # @param client [DXLClient::Client]
    def initialize(service_manager, client)
      @logger = DXLClient::Logger.logger(self.class.name)
      @client = client
      @manager = service_manager
    end

    def on_connect
      @manager.services_lock.synchronize do
        all_services_registered = @manager.services.values.all? do |service|
          @logger.debug("Re-registering service: #{service.service_type}")
          register_service_if_connected(service)
        end
        @manager.services_ttl_condition.broadcast if all_services_registered
      end
    end

    # @param service [DXLClient::ServiceRegistrationInfo]
    def register_service_request(service)
      DXLClient::Request.new(
        DXL_SERVICE_REGISTER_REQUEST_TOPIC
      ).tap do |request|
        request.payload = JSON.dump(serviceType: service.service_type,
                                    metaData: service.metadata,
                                    requestChannels: service.topics,
                                    ttlMins: service.ttl,
                                    serviceGuid: service.service_id)
        request.destination_tenant_guids = service.destination_tenant_guids
      end
    end

    def run
      @manager.services_lock.synchronize do
        process_service_ttls while @manager.services_ttl_loop_continue
      end
    end

    private

    def process_service_ttls
      min_service_ttl = minimum_service_ttl
      if min_service_ttl
        if min_service_ttl > 0
          @manager.services_ttl_condition.wait(@manager.services_lock,
                                               min_service_ttl)
        end
      else
        @manager.services_ttl_condition.wait(@manager.services_lock)
      end
    end

    def minimum_service_ttl
      @manager.services.values.reduce(nil) do |min_so_far, service|
        min_so_far = compare_service_ttl(min_so_far, service)
        break if min_so_far == false
        min_so_far
      end
    end

    # @param service [DXLClient::ServiceRegistrationInfo]
    def compare_service_ttl(min_so_far, service)
      ttl_remaining = reregister_service_if_ttl_expired(
        service_ttl_remaining(service),
        service
      )
      if min_so_far.nil? || !ttl_remaining == false ||
         ttl_remaining < min_so_far
        ttl_remaining
      else
        min_so_far
      end
    end

    def reregister_service_if_ttl_expired(ttl_remaining, service)
      if ttl_remaining <= 0
        @logger.debug(
          "TTL expired, re-registering service: #{service.service_type}"
        )
        ttl_remaining = service.ttl if register_service_if_connected(service)
      end
      ttl_remaining
    end

    def service_ttl_remaining(service)
      service_ttl_seconds = service.ttl * 60
      if service.last_registration.nil?
        0
      else
        now = Time.now
        service.last_registration = now if now < service.last_registration
        service.last_registration - now + service_ttl_seconds
      end
    end

    def register_service_if_connected(service)
      register_service(service)
      true
    rescue MQTT::NotConnectedException
      @logger.errorf(
        'Error registering service %s, client disconnected',
        service.service_type
      )
      false
    end

    def register_service(service)
      request = register_service_request(service)
      response = @client.sync_request(request)
      handle_register_service_response(service, response)
    rescue StandardError => e
      @logger.errorf(
        'Error registering service %s: %s. Code: %s.',
        service.service_type, e.class.name, e.message
      )
      false
    end

    # @param service [DXLClient::ServiceRegistrationInfo]
    def handle_register_service_response(service, response)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        @logger.errorf(
          'Error registering service %s: %s. Code: %s.',
          service.service_type, response.error_message, response.error_code
        )
        false
      else
        service.last_registration = Time.now
        true
      end
    end
  end

  private_constant :ServiceWorker
end
