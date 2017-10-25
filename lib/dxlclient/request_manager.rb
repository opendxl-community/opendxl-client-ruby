require 'set'
require 'thread'
require 'timeout'

module DXLClient
  class RequestManager
    def initialize(client)
      @client = client

      @request_lock = Mutex.new
      @request_condition = ConditionVariable.new
      @requests = {}
      @responses = {}
    end

    def on_response(response)
      request_message_id = response.request_message_id
      response_callback = nil

      @request_lock.synchronize do
        response_callback = @requests[request_message_id]
        if response_callback
          @requests.delete(request_message_id)
        else
          @responses[request_message_id] = response
          @request_condition.broadcast
        end
      end

      if response_callback
        response_callback.on_response(response)
      end
    end

    def sync_request(request, timeout)
      register_request(request, nil)
      begin
        @client.send_request(request)
        wait_for_response(request, timeout)
      ensure
        unregister_request(request)
      end
    end

    def async_request(request, response_callback=nil)
      register_request(request, response_callback)
      begin
        @client.send_request(request)
      rescue
        unregister_request(request)
        raise
      end
    end

    private

    def register_request(request, response_callback)
      @request_lock.synchronize do
        @requests[request.message_id] = response_callback
      end
    end

    def unregister_request(request)
      @request_lock.synchronize do
        @requests.delete(request.message_id)
        @responses.delete(request.message_id)
      end
    end

    def wait_for_response(request, timeout)
      message_id = request.message_id
      @request_lock.synchronize do
        wait_start = Time.now
        until @responses.include?(message_id)
          now = Time.now
          if now < wait_start
            wait_start = now
          end
          wait_time_remaining = wait_start - now + timeout
          if wait_time_remaining <= 0
            raise Timeout::Error,
                  "Timeout waiting for response to message: #{message_id}"
          end
          @request_condition.wait(@request_lock,
                                  wait_time_remaining)
        end
        @responses[message_id]
      end
    end
  end

  private_constant :RequestManager
end
