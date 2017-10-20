require 'set'

module DXLClient
  class RequestManager
    def initialize(client)
      @client = client

      @sync_wait_message_lock = Mutex.new
      @sync_wait_message_condition = ConditionVariable.new
      @sync_wait_message_ids = Set.new
      @sync_wait_message_responses = {}

      @current_request_message_lock = Mutex.new
      @current_request_message_ids = Set.new
    end

    def on_response(response)
      request_message_id = response.request_message_id
      begin
        @sync_wait_message_lock.synchronize do
          @sync_wait_message_ids.delete(request_message_id)
          @sync_wait_message_responses[request_message_id] = response
          @sync_wait_message_condition.broadcast
        end
      ensure
        remove_current_request(request_message_id)
      end
    end

    def sync_request(request, timeout)
      register_wait_for_response(request)
      begin
        add_current_request(request.message_id)
        @client.send_request(request)
        wait_for_response(request, timeout)
      ensure
        unregister_wait_for_response(request)
      end
    end

    private

    def add_current_request(message_id)
      @current_request_message_lock.synchronize do
        @current_request_message_ids.add(message_id)
      end
    end

    def remove_current_request(message_id)
      @current_request_message_lock.synchronize do
        @current_request_message_ids.delete(message_id)
      end
    end

    def register_wait_for_response(request)
      @sync_wait_message_lock.synchronize do
        @sync_wait_message_ids.add(request.message_id)
      end
    end

    def unregister_wait_for_response(request)
      @sync_wait_message_lock.synchronize do
        @sync_wait_message_ids.delete(request.message_id)
        @sync_wait_message_responses.delete(request.message_id)
      end
    end

    def wait_for_response(request, timeout)
      message_id = request.message_id
      @sync_wait_message_lock.synchronize do
        wait_start = Time.now
        while !@sync_wait_message_responses.include?(message_id)
          now = Time.now
          if now < wait_start
            wait_start = now
          end
          wait_time_remaining = wait_start - now + timeout
          if wait_time_remaining <= 0
            raise DXLClient::WaitTimeoutError.
                new("Timeout waiting for response to message: #{message_id}")
          end
          @sync_wait_message_condition.wait(@sync_wait_message_lock,
                                          wait_time_remaining)
        end
        @sync_wait_message_responses.delete(message_id)
      end
    end
  end

  private_constant :RequestManager
end
