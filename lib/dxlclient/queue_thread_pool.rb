require 'dxlclient/callback_info'
require 'dxlclient/error'
require 'dxlclient/logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Pool of threads consuming tasks from a queue
  class QueueThreadPool
    def initialize(queue_size, num_threads, thread_prefix)
      @logger = DXLClient::Logger.logger(self.class.name)
      @queue_size = queue_size
      @thread_prefix = thread_prefix
      @task_queue = SizedQueue.new(queue_size)
      @task_threads = create_task_threads(num_threads)
      @destroy_lock = Mutex.new
      @pool_alive = true
    end

    def add_task(&block)
      raise ArgumentError, 'Block not given for task' unless block
      @destroy_lock.synchronize do
        unless @pool_alive
          raise DXLClient::Error::ShutdownError,
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

    def create_task_threads(num_threads)
      @logger.debugf('Creating thread pool %s. Threads: %d. Queue depth: %s.',
                     @thread_prefix, num_threads, @queue_size)
      @task_threads = Array.new(num_threads) do |thread_id|
        Thread.new { thread_run(thread_id + 1) }
      end
    end

    def thread_run(thread_id)
      Thread.current.name = "#{@thread_prefix}-#{thread_id}"
      @logger.debugf('Starting thread: %s', Thread.current.name)
      begin
        process_tasks_until_done
      ensure
        @task_queue.push(:done)
        @logger.debugf('Ending thread: %s', Thread.current.name)
      end
    end

    def process_tasks_until_done
      task = @task_queue.pop
      until task == :done
        process_task(task)
        task = @task_queue.pop
      end
    end

    def process_task(task)
      if task.is_a?(Proc)
        begin
          task.call
        rescue StandardError => e
          @logger.exception(e, 'Error running task')
        end
      else
        @logger.errorf('Unknown task type dequeued: %s', task)
      end
    end
  end
  private_constant :QueueThreadPool
end
