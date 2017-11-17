require 'dxlclient/callback_info'
require 'dxlclient/dxl_error'

module DXLClient
  class QueueThreadPool
    def initialize(queue_size, num_threads, thread_prefix)
      @logger = DXLClient::Logger.logger(self.class.name)
      @queue_size = queue_size
      @thread_prefix = thread_prefix
      @task_queue = SizedQueue.new(queue_size)
      @logger.debugf('Creating thread pool %s. Threads: %d. Queue depth: %s.',
                     thread_prefix, num_threads, queue_size)
      @task_threads = Array.new(num_threads) do |thread_id|
        Thread.new { thread_run(thread_id + 1) }
      end
      @destroy_lock = Mutex.new
      @pool_alive = true
    end

    def add_task(&block)
      raise ArgumentError, 'Block not given for task' unless block
      @destroy_lock.synchronize do
        unless @pool_alive
          raise DXLClient::ShutdownError,
                format('Thread pool %s has already been destroyed, %s',
                       @thread_prefix, 'cannot add new task')
        end
        @task_queue.push(block)
      end
    end

    def destroy
      @destroy_lock.synchronize do
        return unless @task_threads.any?(&:alive?)
        @logger.debugf('Destroying thread pool %s...', @thread_prefix)
        @task_queue.push(:done)
        @task_threads.each(&:join)
        @logger.debugf('Thread pool %s destroyed', @thread_prefix)
        @pool_alive = false
      end
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
