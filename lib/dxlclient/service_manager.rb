require 'json'
require 'set'

require 'dxlclient/error'
require 'dxlclient/message/request'
require 'dxlclient/service_worker'
require 'dxlclient/util'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # rubocop: disable ClassLength

  # Manager which handles service registrations with the DXL fabric.
  class ServiceManager
    DXL_SERVICE_UNREGISTER_REQUEST_TOPIC =
      '/mcafee/service/dxl/svcregistry/unregister'.freeze

    SERVICE_REGISTRATION_REQUEST_TIMEOUT = 10 # seconds
    SERVICE_UNREGISTRATION_REQUEST_TIMEOUT = 60 # seconds

    private_constant :DXL_SERVICE_UNREGISTER_REQUEST_TOPIC,
                     :SERVICE_REGISTRATION_REQUEST_TIMEOUT,
                     :SERVICE_UNREGISTRATION_REQUEST_TIMEOUT

    attr_reader :services_lock, :services_ttl_condition,
                :services_ttl_loop_continue, :services

    # @param client [DXLClient::Client]
    def initialize(client)
      @logger = DXLClient::Logger.logger(self.class.name)

      @client = client
      @services = {}

      @services_lock = Mutex.new
      @worker = ServiceWorker.new(self, client)

      @services_ttl_condition = ConditionVariable.new
      @services_ttl_loop_continue = true
      @services_ttl_thread = create_worker_thread
    end

    def destroy
      @logger.debug('Destroying service manager...')
      @services_lock.synchronize do
        @services_ttl_loop_continue = false
      end
      @services_ttl_condition.broadcast
      @services_ttl_thread.join
      remove_all_registered_services
      @logger.debug('Service manager destroyed')
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_async(service_reg_info)
      request = @worker.register_service_request(service_reg_info)
      @client.async_request(request) do
        add_service_callbacks(service_reg_info)
      end
      add_service_entry(service_reg_info)
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_sync(service_reg_info,
                         timeout = SERVICE_REGISTRATION_REQUEST_TIMEOUT)
      request = @worker.register_service_request(service_reg_info)
      response = @client.sync_request(request, timeout)
      if response.message_type == DXLClient::Message::MESSAGE_TYPE_ERROR
        raise DXLClient::Error::DXLError,
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
      @worker.on_connect
    end

    private

    def create_worker_thread
      Thread.new do
        DXLClient::Util.current_thread_name(
          "DXLServiceWorker-#{@client.object_id}"
        )
        @logger.debug('Services ttl monitor thread started')
        @worker.run
        @logger.debug('Services ttl monitor thread terminating')
      end
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def add_service_callbacks(service_reg_info)
      service_reg_info.topics.each do |topic|
        service_reg_info.callbacks(topic).each do |callback|
          @client.add_request_callback(topic, callback, true)
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
        raise DXLClient::Error::DXLError, error
      end
      # rubocop: enable GuardClause
    end

    # @param service_reg_info [DXLClient::ServiceRegistrationInfo]
    def unregister_service_request(service_reg_info)
      request = DXLClient::Message::Request.new(
        DXL_SERVICE_UNREGISTER_REQUEST_TOPIC
      )
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
