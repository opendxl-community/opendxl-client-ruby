require 'dxlclient/logger/named_logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Logger to which all code under the DXLClient module logs messages.
  module Logger
    # Named logger which uses Ruby's built-in logger to log messages.
    class StdlibNamedLogger < DXLClient::Logger::NamedLogger
      # @param name [String]
      # @param level [Integer]
      # @param stdlib_root_logger [StdlibRootLogger]
      # @param logger [Logger]
      def initialize(name, level, stdlib_root_logger, logger)
        super(name, level)
        @logger = logger
        @stdlib_root_logger = stdlib_root_logger
        @level_lock = Mutex.new
      end

      def add(level, message)
        @logger.add(level, message, @name) if level >= @level
      end

      def debug(message)
        add(DXLClient::Logger::DEBUG, message)
      end

      def debugf(*args)
        add(DXLClient::Logger::DEBUG, format(*args))
      end

      def error(message)
        add(DXLClient::Logger::ERROR, message)
      end

      def errorf(*args)
        add(DXLClient::Logger::ERROR, format(*args))
      end

      def info(message)
        add(DXLClient::Logger::INFO, message)
      end

      def infof(*args)
        add(DXLClient::Logger::INFO, format(*args))
      end

      def warn(message)
        add(DXLClient::Logger::WARN, message)
      end

      def warnf(*args)
        add(DXLClient::Logger::WARN, format(*args))
      end

      def exception(exception, message = nil)
        log_message = StringIO.new

        log_message << "#{message}: " if message

        log_message << exception.message
        log_message << " (#{exception.class})"

        exception.backtrace.each do |line|
          log_message << "\n        from #{line}"
        end
        log_message << "\n"

        error(log_message.string)
      end

      def level
        @level_lock.synchronize { @level }
      end

      def level=(level)
        @level_lock.synchronize { @level = level }
        @stdlib_root_logger.update_level
      end
    end

    private_constant :StdlibNamedLogger
  end
end
