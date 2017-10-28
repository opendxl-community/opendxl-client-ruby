require 'json'
require 'set'

require 'dxlclient/dxl_error'

module DXLClient
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
      @logger = DXLClient::Logger.logger(self.class)

      @client = client
      @services = {}

      @services_lock = Mutex.new
      @services_ttl_condition = ConditionVariable.new
      @services_ttl_thread = Thread.new { services_ttl_loop }
      @services_ttl_loop_continue = true
    end

    def destroy
      @logger.debug('Destroying service manager...')
      services_to_destroy = nil
      @services_lock.synchronize do
        services_to_destroy = @services.values
      end
      services_to_destroy.each do |service_reg_info|
        remove_service_sync(service_reg_info)
      end
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
      request = unregister_service_request(service_reg_info)
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::DXLError,
              format('Error unregistering service %s: %s. Code: %s',
                     service_reg_info.service_type, res.error_message,
                     res.error_code)
      end
      remove_service_callbacks(service_reg_info)
      @services_lock.synchronize do
        @services.delete(service_reg_info.service_id)
      end
    end

    def on_connect
      @services_lock.synchronize do
        connected = true
        @services.each_value do |service|
          begin
            register_service_sync(service)
          rescue MQTT::NotConnectedException
            @logger.errorf(
              'Error registering service %s, client disconnected',
              service.service_type
            )
            connected = false
          end
        end
        @services_ttl_condition.broadcast if connected
      end
    end

    private

    def services_ttl_loop
      @logger.debug('Services ttl monitor thread started')

      @services_lock.synchronize do
        while @services_ttl_loop_continue
          if @services.empty?
            @services_ttl_condition.wait(@services_lock)
          else
            connected = true
            min_service_ttl =
              @services.values.reduce(nil) do |min_so_far, service|
                service_ttl_seconds = service.ttl * 60
                if service.last_registration.nil?
                  ttl_remaining = 0
                else
                  now = Time.now
                  if now < service.last_registration
                    service.last_registration = now
                  end
                  ttl_remaining = service.last_registration - now +
                                  service_ttl_seconds
                end
                if ttl_remaining <= 0
                  begin
                    register_service_sync(service)
                  rescue MQTT::NotConnectedException
                    @logger.errorf(
                      'Error registering service %s, client disconnected',
                      service.service_type
                    )
                    connected = false
                    break
                  end
                  ttl_remaining = service_ttl_seconds
                end
                if min_so_far.nil? || ttl_remaining < min_so_far
                  ttl_remaining
                else
                  min_so_far
                end
              end
            if connected
              wait_time = min_service_ttl < 5 ? 5 : min_service_ttl
              @services_ttl_condition.wait(
                @services_lock,
                wait_time < 5 ? 5 : wait_time
              )
            else
              @services_ttl_condition.wait(@services_lock)
            end
          end
        end
      end

      @logger.debug('Services ttl monitor thread terminating')
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def register_service_request(service_reg_info)
      request = DXLClient::Request.new(DXL_SERVICE_REGISTER_REQUEST_TOPIC)
      request.payload = JSON.dump(serviceType: service_reg_info.service_type,
                                  metaData: service_reg_info.metadata,
                                  requestChannels: service_reg_info.topics,
                                  ttlMins: service_reg_info.ttl,
                                  serviceGuid: service_reg_info.service_id)
      request.destination_tenant_guids =
        service_reg_info.destination_tenant_guids
      request
    end

    def register_service_sync(service_reg_info)
      request = register_service_request(service_reg_info)
      response = @client.sync_request(request)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        @logger.errorf(
          'Error registering service %s: %s. Code: %s.',
          service_reg_info.service_type, response.error_message,
          response.error_code
        )
        false
      else
        service_reg_info.last_registration = Time.now
        true
      end
    rescue StandardError => e
      @logger.errorf(
        'Error registering service %s: %s. Code: %s.',
        service_reg_info.service_type, e.class.name, e.message
      )
      false
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
