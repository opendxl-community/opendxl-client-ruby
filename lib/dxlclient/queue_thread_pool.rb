require 'dxlclient/callback_info'

module DXLClient
  class QueueThreadPool
    def initialize(queue_size, num_threads, thread_prefix)
      @logger = DXLClient::Logger.logger(self.class.name)
      @queue_size = queue_size
      @thread_prefix = thread_prefix
      @task_queue = SizedQueue.new(queue_size)
      @logger.debugf('Creating thread pool. Threads: %d. Queue depth: %s.',
                     num_threads, queue_size)
      @task_threads = Array.new(num_threads) do |thread_id|
        Thread.new { thread_run(thread_id + 1) }
      end
    end

    def add_task(&block)
      raise ArgumentError, 'Block not given for task' unless block
      @task_queue.push(block)
    end

    def shutdown
      return unless @task_threads.any?(&:alive?)
      @task_queue.push(:done)
      @task_threads.each(&:join)
    end

    private

    def thread_run(thread_id)
      Thread.current.name = "#{@thread_prefix}-#{thread_id}"
      @logger.debugf('Starting thread: %s', Thread.current.name)
      begin
        task = @task_queue.pop
        until task == :done
          if task.is_a?(Proc)
            begin
              task.call
            rescue StandardError => e
              @logger.exception(e, 'Error running task')
            end
          else
            @logger.errorf('Unknown task type dequeued: %s', task)
          end
          task = @task_queue.pop
        end
      ensure
        @task_queue.push(:done)
        @logger.debugf('Ending thread: %s', Thread.current.name)
      end
    end
  end
  private_constant :QueueThreadPool
end
