require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'dxlclient/service_registration_info'
require 'integration/client_helpers'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'registered services', :integration do
  class RegisteredServices
    MAX_WAIT = 8
    DXL_SERVICE_REGISTER_EVENT_TOPIC = \
      '/mcafee/event/dxl/svcregistry/register'.freeze
    DXL_SERVICE_UNREGISTER_EVENT_TOPIC = \
      '/mcafee/event/dxl/svcregistry/unregister'.freeze

    def initialize(client)
      @client = client
      @services = Set.new
      @services_mutex = Mutex.new
      @services_condition = ConditionVariable.new
      add_register_event_callback
      add_unregister_event_callback
    end

    def add_register_event_callback
      @client.add_event_callback(DXL_SERVICE_REGISTER_EVENT_TOPIC) do |event|
        @services_mutex.synchronize do
          @services.add(
            ClientHelpers.message_payload_as_json(event)['serviceGuid']
          )
          @services_condition.broadcast
        end
      end
    end

    def add_unregister_event_callback
      @client.add_event_callback(DXL_SERVICE_UNREGISTER_EVENT_TOPIC) do |event|
        @services_mutex.synchronize do
          @services.delete(
            ClientHelpers.message_payload_as_json(event)['serviceGuid']
          )
          @services_condition.broadcast
        end
      end
    end

    def wait_for_registered_service(service)
      wait_for_service(-> { !@services.include?(service) })
    end

    def wait_for_unregistered_service(service)
      wait_for_service(-> { @services.include?(service) })
    end

    def registered?(service)
      @services_mutex.synchronize { @services.include?(service) }
    end

    def wait_for_service(proc)
      @services_mutex.synchronize do
        ClientHelpers.while_not_done_and_time_remaining(
          proc, MAX_WAIT
        ) do |wait_remaining|
          @services_condition.wait(@services_mutex, wait_remaining)
        end
      end
    end
  end

  def with_service_callbacks
    ClientHelpers.with_integration_client do |client|
      service_info = DXLClient::ServiceRegistrationInfo.new(
        client, 'register_service_spec_service'
      )
      service_info.add_topic(
        "register_service_spec_topic_1/#{service_info.service_id}"
      ) do |request|
        response = DXLClient::Message::Response.new(request)
        response.payload = 'Ok'
        client.send_response(response)
      end

      yield(client, service_info)
    end
  end

  context 'when service registered before connect' do
    it 'should register service properly with broker' do
      with_service_callbacks do |client, service_info|
        service_id = service_info.service_id
        registered_services = RegisteredServices.new(client)

        client.register_service_async(service_info)
        expect(registered_services.registered?(service_id)).to be false
        client.connect

        registered_services.wait_for_registered_service(service_id)
        expect(registered_services.registered?(service_id)).to be true

        client.unregister_service_sync(
          service_info, ClientHelpers::SERVICE_REGISTRATION_WAIT
        )
        registered_services.wait_for_unregistered_service(service_id)
        expect(registered_services.registered?(service_id)).to be false
      end
    end
  end

  context 'when service registered after connect' do
    it 'should register service properly with broker' do
      with_service_callbacks do |client, service_info|
        service_id = service_info.service_id
        registered_services = RegisteredServices.new(client)

        expect(registered_services.registered?(service_id)).to be false
        client.connect

        client.register_service_sync(
          service_info, ClientHelpers::SERVICE_REGISTRATION_WAIT
        )
        registered_services.wait_for_registered_service(service_id)
        expect(registered_services.registered?(service_id)).to be true

        client.unregister_service_sync(
          service_info, ClientHelpers::SERVICE_REGISTRATION_WAIT
        )
        registered_services.wait_for_unregistered_service(service_id)
        expect(registered_services.registered?(service_id)).to be false
      end
    end
  end

  context 'when client never connected' do
    it 'should never register service with broker' do
      with_service_callbacks do |client, service_info|
        service_id = service_info.service_id
        registered_services = RegisteredServices.new(client)

        expect(registered_services.registered?(service_id)).to be false
        client.register_service_sync(
          service_info, ClientHelpers::SERVICE_REGISTRATION_WAIT
        )
        expect(registered_services.registered?(service_id)).to be false

        client.unregister_service_sync(
          service_info, ClientHelpers::SERVICE_REGISTRATION_WAIT
        )
        expect(registered_services.registered?(service_id)).to be false
      end
    end
  end

  context 'when service unregistered before connect' do
    it 'should never register service with broker' do
      with_service_callbacks do |client, service_info|
        service_id = service_info.service_id
        registered_services = RegisteredServices.new(client)

        expect(registered_services.registered?(service_id)).to be false
        client.register_service_sync(
          service_info, ClientHelpers::SERVICE_REGISTRATION_WAIT
        )
        expect(registered_services.registered?(service_id)).to be false

        client.unregister_service_sync(
          service_info, ClientHelpers::SERVICE_REGISTRATION_WAIT
        )
        expect(registered_services.registered?(service_id)).to be false

        client.connect
        expect(registered_services.registered?(service_id)).to be false
      end
    end
  end

  context 'when service registered with broker' do
    it 'should be able to send a request' do
      with_service_callbacks do |client, service_info|
        service_id = service_info.service_id
        registered_services = RegisteredServices.new(client)

        client.register_service_async(service_info)
        client.connect
        expect(registered_services.registered?(service_id)).to be true

        request = DXLClient::Message::Request.new(service_info.topics[0])
        request.payload = 'Test'
        response = client.sync_request(request)
        expect(response.payload).to eql('Ok')
      end
    end
  end
end
