require 'set'
require 'thread'
require 'timeout'

require 'dxlclient/response_callback'

# Module under which all of the DXL client functionality resides.
module DXLClient
  #  Manager that tracks outstanding requests and notifies the appropriate
  #  parties (invoking a response callback, notifying a waiting object, etc.)
  #  when a corresponding response is received.
  class RequestManager < ResponseCallback
    # @param client [DXLClient::Client]
    def initialize(client, reply_to_topic)
      @logger = DXLClient::Logger.logger(self.class.name)
      @client = client

      @reply_to_topic = reply_to_topic
      @services_lock = Mutex.new
      @services_ttl_condition = ConditionVariable.new
      @requests = {}
      @responses = {}

      @client.add_response_callback(reply_to_topic, self)
    end

    def destroy
      @client.remove_response_callback(@reply_to_topic, self)
    end

    # @param response [DXLClient::Response]
    def on_response(response)
      request_message_id = response.request_message_id
      @logger.debug(
        "Received response. Request message id: #{request_message_id}."
      )
      response_callback = nil

      @services_lock.synchronize do
        response_callback = deliver_response(request_message_id, response)
      end
      response.invoke_callback(response_callback)
    end

    def sync_request(request, timeout)
      register_request(request, nil)
      begin
        @client.send_request(request)
        wait_for_matching_response(request, timeout)
      ensure
        unregister_request(request)
      end
    end

    def async_request(request, response_callback = nil)
      register_request(request, response_callback)
      begin
        @client.send_request(request)
      rescue MQTT::NotConnectedException, SocketError
        unregister_request(request)
        raise
      end
    end

    private

    def deliver_response(request_message_id, response)
      @requests[request_message_id].tap do |response_callback|
        if response_callback
          @requests.delete(request_message_id)
        else
          @responses[request_message_id] = response
          @services_ttl_condition.broadcast
        end
      end
    end

    def register_request(request, response_callback)
      @services_lock.synchronize do
        @requests[request.message_id] = response_callback
      end
    end

    def unregister_request(request)
      @services_lock.synchronize do
        @requests.delete(request.message_id)
        @responses.delete(request.message_id)
      end
    end

    def wait_for_matching_response(request, timeout)
      message_id = request.message_id
      @services_lock.synchronize do
        wait_start = Time.now
        until @responses.include?(message_id)
          now = Time.now
          wait_start = now if now < wait_start
          wait_for_next_response(wait_start, now, message_id, timeout)
        end
        @responses[message_id]
      end
    end

    def wait_for_next_response(wait_start, now, message_id, timeout)
      wait_time_remaining = wait_start - now + timeout
      if wait_time_remaining <= 0
        raise Timeout::Error,
              "Timeout waiting for response to message: #{message_id}"
      end
      @services_ttl_condition.wait(@services_lock, wait_time_remaining)
    end
  end

  private_constant :RequestManager
end
