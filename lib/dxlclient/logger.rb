require 'logger'

module DXLClient
  module Logger
    DEBUG = ::Logger::DEBUG
    ERROR = ::Logger::ERROR
    INFO = ::Logger::INFO
    WARN = ::Logger::WARN

    def self.logger(name)
      root_logger().logger(name)
    end

    def self.root_logger()
      @@root_logger ||= StdlibConsoleRootLogger.new
    end

    def self.root_logger=(logger)
      @@root_logger = logger
    end

    class RootLogger
      attr_accessor :level

      def initialize
        @level = Logger::INFO
      end

      def logger(name)
        raise NotImplementedError
      end
    end

    class NamedLogger
      def debug(message)
        raise NotImplementedError
      end

      def error(message)
        raise NotImplementedError
      end

      def info(message)
        raise NotImplementedError
      end
    end

    private

    class StdlibConsoleRootLogger < RootLogger
      def initialize
        super
        @logger = ::Logger.new(STDOUT)
        @logger.level = @level
      end

      def logger(name)
        StdlibNamedLogger.new(@logger, name, @level)
      end
    end

    class StdlibNamedLogger < NamedLogger
      attr_accessor :level

      def initialize(parent_logger, logger_name, level)
        @parent_logger = parent_logger
        @logger_name = logger_name
        @level = level
      end

      def debug(message)
        @parent_logger.debug(@logger_name) { message }
      end

      def error(message)
        @parent_logger.error(@logger_name) { message }
      end

      def info(message)
        @parent_logger.info(@logger_name) { message }
      end

      def warn(mesasge)
        @parent_logger.warn(@logger_name) { message }
      end

      def level=(level)
        @parent_logger.level = level
      end
    end

    private_constant :StdlibConsoleRootLogger, :NamedLogger
  end
end
