require 'logger'

module DXLClient
  module Logger
    DEBUG = ::Logger::DEBUG
    ERROR = ::Logger::ERROR
    INFO = ::Logger::INFO
    WARN = ::Logger::WARN

    @root_logger = nil
    @root_logger_lock = Mutex.new

    def self.logger(name, level = nil)
      root_logger.logger(name, level)
    end

    # @return [DXLClient::RootLogger]
    def self.root_logger
      @root_logger_lock.synchronize { @root_logger ||= StdlibRootLogger.new }
    end

    def self.root_logger=(logger)
      @root_logger_lock.synchronize { @root_logger = logger }
    end

    class RootLogger
      attr_accessor :level, :log_device

      def initialize
        @level = Logger::INFO
        @log_device = STDOUT
      end

      # @return [DXLClient::NamedLogger]
      def logger(_name, _level = nil)
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

      def debug(_message)
        raise NotImplementedError
      end

      def debugf(_message)
        raise NotImplementedError
      end

      def error(_message)
        raise NotImplementedError
      end

      def errorf(_message)
        raise NotImplementedError
      end

      def info(_message)
        raise NotImplementedError
      end

      def infof(_message)
        raise NotImplementedError
      end

      def warn(_message)
        raise NotImplementedError
      end

      def warnf(_message)
        raise NotImplementedError
      end

      def exception(exception, message = nil)
        raise NotImplementedError
      end
    end

    class StdlibRootLogger < RootLogger
      def initialize
        super
        @logger = nil
        @named_loggers = {}
        @logger_lock = Mutex.new
      end

      def logger(name, level = nil)
        log = nil
        @logger_lock.synchronize do
          unless @logger
            @logger = ::Logger.new(@log_device)
            @logger.level = @level
          end
          log = @named_loggers[name]
          if log
            log.level = level if level
          else
            log = StdlibNamedLogger.new(name,
                                        level ? level : @level,
                                        self,
                                        @logger)
            @named_loggers[name] = log
          end
        end
        update_level
        log
      end

      def level
        @logger_lock.synchronize { @level }
      end

      def level=(level)
        @logger_lock.synchronize { @level = level }
        update_level
      end

      def log_device
        @logger_lock.synchronize { @log_device }
      end

      def log_device=(log_device)
        @logger_lock.synchronize do
          if @logger
            raise DXLClient::DXLError,
                  'Log device cannot be set after logger created'
          end
          @log_device = log_device
        end
      end

      def update_level
        @logger_lock.synchronize do
          if @logger
            min_level_named_logger = @named_loggers.values.min_by(&:level)
            if min_level_named_logger && min_level_named_logger.level <= @level
              @logger.level = min_level_named_logger.level
            else
              @logger.level = @level
            end
          end
        end
      end
    end

    class StdlibNamedLogger < NamedLogger
      # @param [StdlibRootLogger] stdlib_root_logger
      # @param [Logger] logger
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
        add(DEBUG, message)
      end

      def debugf(*args)
        add(DEBUG, format(*args))
      end

      def error(message)
        add(ERROR, message)
      end

      def errorf(*args)
        add(ERROR, format(*args))
      end

      def info(message)
        add(INFO, message)
      end

      def infof(*args)
        add(INFO, format(*args))
      end

      def warn(message)
        add(WARN, message)
      end

      def warnf(*args)
        add(WARN, format(*args))
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

    private_constant :StdlibRootLogger, :StdlibNamedLogger
  end
end
