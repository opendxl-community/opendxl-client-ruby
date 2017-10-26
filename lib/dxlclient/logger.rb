require 'logger'

module DXLClient
  module Logger
    DEBUG = ::Logger::DEBUG
    ERROR = ::Logger::ERROR
    INFO = ::Logger::INFO
    WARN = ::Logger::WARN

    def self.logger(name)
      root_logger.logger(name)
    end

    def self.root_logger
      @root_logger ||= StdlibConsoleRootLogger.new
    end

    def self.root_logger=(logger)
      @root_logger = logger
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
      attr_accessor :level
      attr_reader :name

      def initialize(name, level)
        @name = name
        @level = level
      end

      def debug(message)
        raise NotImplementedError
      end

      def debugf(message)
        raise NotImplementedError
      end

      def error(message)
        raise NotImplementedError
      end

      def errorf(message)
        raise NotImplementedError
      end

      def info(message)
        raise NotImplementedError
      end

      def infof(message)
        raise NotImplementedError
      end

      def warn(message)
        raise NotImplementedError
      end

      def warnf(message)
        raise NotImplementedError
      end
    end

    class StdlibConsoleRootLogger < RootLogger
      def initialize
        super
      end

      def logger(name)
        StdlibNamedLogger.new(::Logger.new(STDOUT),
                              name,
                              @level)
      end

      def level=(level)
        @level = level
      end
    end

    class StdlibNamedLogger < NamedLogger
      def initialize(parent, name, level)
        super(name, level)
        @parent = parent
        @parent.level = level
      end

      def debug(message)
        @parent.debug(@name) { message }
      end

      def debugf(*args)
        @parent.debug(@name) { format(*args) }
      end

      def error(message)
        @parent.error(@name) { message }
      end

      def errorf(*args)
        @parent.error(@name) { format(*args) }
      end

      def info(message)
        @parent.info(@name) { message }
      end

      def infof(*args)
        @parent.info(@name) { format(*args) }
      end

      def warn(message)
        @parent.warn(@name) { message }
      end

      def warnf(*args)
        @parent.warn(@name) { format(*args) }
      end

      def exception(exception, message=nil)
        log_message = StringIO.new

        if message
          log_message << "#{message}: "
        end

        log_message << exception.message
        log_message << " (#{exception.class})"

        exception.backtrace.each do |line|
          log_message << "\n        from #{line}"
        end
        log_message << "\n"

        error(log_message.string)
      end

      def level
        @parent.level
      end

      def level=(level)
        @parent.level = level
      end
    end

    private_constant :StdlibConsoleRootLogger, :NamedLogger
  end
end
