require 'set'

module DxlClient
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

    def sync_request(request, timeout)
      response = nil
      register_wait_for_response(request)
      begin
        add_current_request(request.message_id)
        @client.send_request(request)
        wait_for_response(request, timeout)
      ensure
        unregister_wait_for_response(request)
      end
      response
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
        wait_time_remaining = timeout

        while !@sync_wait_message_responses.include?(message_id) &&
          wait_time_remaining > 0
          @sync_wait_message_condition.wait(@sync_wait_message_lock,
                                            wait_time_remaining)
          wait_time_remaining = Time.now - wait_start + timeout
        end

        if @sync_wait_message_responses.include?(message_id)
          @sync_wait_message_responses.delete(message_id)
        else
          raise Exception("crap")
        end
      end
    end
  end
end
