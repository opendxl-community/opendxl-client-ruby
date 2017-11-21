require 'json'
require 'set'

require 'dxlclient/dxl_error'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Manager which handles service registrations with the DXL fabric. The
  # manager re-registers locally registered services with the DXL fabric as the
  # service's Time-To-Live value expires. The manager also re-registers
  # services with the DXL fabric as disconnect / reconnect events occur.
  class ServiceManager
    DXL_SERVICE_REGISTER_REQUEST_TOPIC = \
      '/mcafee/service/dxl/svcregistry/register'.freeze
    DXL_SERVICE_UNREGISTER_REQUEST_TOPIC =
      '/mcafee/service/dxl/svcregistry/unregister'.freeze

    SERVICE_REGISTRATION_REQUEST_TIMEOUT = 10 # seconds
    SERVICE_UNREGISTRATION_REQUEST_TIMEOUT = 60 # seconds

    private_constant :DXL_SERVICE_REGISTER_REQUEST_TOPIC,
                     :DXL_SERVICE_UNREGISTER_REQUEST_TOPIC,
                     :SERVICE_REGISTRATION_REQUEST_TIMEOUT,
                     :SERVICE_UNREGISTRATION_REQUEST_TIMEOUT

    # @param client [DXLClient::Client]
    def initialize(client)
      @logger = DXLClient::Logger.logger(self.class.name)

      @client = client
      @services = {}

      @services_lock = Mutex.new
      @services_ttl_condition = ConditionVariable.new
      @services_ttl_thread = Thread.new do
        Thread.current.name = 'DXLServiceManager'
        services_ttl_loop
      end
      @services_ttl_loop_continue = true
    end

    def destroy
      @logger.debug('Destroying service manager...')
      remove_all_registered_services
      @services_ttl_loop_continue = false
      @services_ttl_condition.broadcast
      @services_ttl_thread.join
      @logger.debug('Service manager destroyed')
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_async(service_reg_info)
      request = register_service_request(service_reg_info)
      @client.async_request(request) do
        add_service_callbacks(service_reg_info)
      end
      add_service_entry(service_reg_info)
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_sync(service_reg_info,
                         timeout = SERVICE_REGISTRATION_REQUEST_TIMEOUT)
      request = register_service_request(service_reg_info)
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::DXLError,
              format('Error registering service %s. Code: %s.',
                     response.error_message, response.error_code)
      end
      add_service_callbacks(service_reg_info)
      add_service_entry(service_reg_info)
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def remove_service_async(service_reg_info)
      request = unregister_service_request(service_reg_info)
      @client.async_request(request) do
        remove_service_callbacks(service_reg_info)
      end
      @services_lock.synchronize do
        @services.delete(service_reg_info.service_id)
      end
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def remove_service_sync(service_reg_info,
                            timeout = SERVICE_UNREGISTRATION_REQUEST_TIMEOUT)
      unregister_service_sync(service_reg_info, timeout)
      remove_service_callbacks(service_reg_info)
      @services_lock.synchronize do
        @services.delete(service_reg_info.service_id)
      end
    end

    def on_connect
      @services_lock.synchronize do
        all_services_registered = @services.values.all? do |service|
          @logger.debug("Re-registering service: #{service.service_type}")
          register_service_if_connected(service)
        end
        @services_ttl_condition.broadcast if all_services_registered
      end
    end

    private

    def services_ttl_loop
      @logger.debug('Services ttl monitor thread started')

      @services_lock.synchronize do
        process_service_ttls while @services_ttl_loop_continue
      end

      @logger.debug('Services ttl monitor thread terminating')
    end

    def process_service_ttls
      min_service_ttl = minimum_service_ttl
      if min_service_ttl
        if min_service_ttl > 0
          @services_ttl_condition.wait(@services_lock, min_service_ttl)
        end
      else
        @services_ttl_condition.wait(@services_lock)
      end
    end

    def minimum_service_ttl
      @services.values.reduce(nil) do |min_so_far, service|
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

    # @param service [DXLClient::ServiceRegistrationInfo]
    def register_service_request(service)
      request = DXLClient::Request.new(DXL_SERVICE_REGISTER_REQUEST_TOPIC)
      request.payload = JSON.dump(serviceType: service.service_type,
                                  metaData: service.metadata,
                                  requestChannels: service.topics,
                                  ttlMins: service.ttl,
                                  serviceGuid: service.service_id)
      request.destination_tenant_guids =
        service.destination_tenant_guids
      request
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

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_callbacks(service_reg_info)
      service_reg_info.topics.each do |topic|
        service_reg_info.callbacks(topic).each do |callback|
          @client.add_request_callback(topic, callback)
        end
      end
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_entry(service_reg_info)
      @services_lock.synchronize do
        @services[service_reg_info.service_id] = service_reg_info
        service_reg_info.last_registration = Time.now
        @services_ttl_condition.broadcast
      end
    end

    def unregister_service_sync(service_reg_info, timeout)
      request = unregister_service_request(service_reg_info)
      response = @client.sync_request(request, timeout)
      # rubocop: disable GuardClause
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        error = format('Error unregistering service %s: %s. Code: %s',
                       service_reg_info.service_type, response.error_message,
                       response.error_code)
        raise DXLClient::DXLError, error
      end
      # rubocop: enable GuardClause
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

    def remove_all_registered_services
      services_to_destroy = nil
      @services_lock.synchronize do
        services_to_destroy = @services.values
      end
      services_to_destroy.each do |service_reg_info|
        remove_service_sync(service_reg_info)
      end
    end
  end

  private_constant :ServiceManager
end
