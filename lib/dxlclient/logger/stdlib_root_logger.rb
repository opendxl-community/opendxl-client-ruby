require 'dxlclient/error'
require 'dxlclient/logger/root_logger'
require 'dxlclient/logger/stdlib_named_logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Logger to which all code under the DXLClient module logs messages.
  module Logger
    # Root logger which uses Ruby's built-in logger to log messages.
    class StdlibRootLogger < DXLClient::Logger::RootLogger
      def initialize
        super
        @root_logger = nil
        @named_loggers = {}
        @logger_lock = Mutex.new
      end

      def logger(name, level = nil)
        log = nil
        @logger_lock.synchronize do
          log = get_named_logger(name, level)
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
            raise DXLClient::Error::DXLError,
                  'Log device cannot be set after logger created'
          end
          @log_device = log_device
        end
      end

      def update_level
        @logger_lock.synchronize do
          logger = root_logger
          min_level_named_logger = @named_loggers.values.min_by(&:level)
          if min_level_named_logger && min_level_named_logger.level <= @level
            logger.level = min_level_named_logger.level
          else
            logger.level = @level
          end
        end
      end

      private

      def get_named_logger(name, level)
        log = @named_loggers[name]
        if log
          log.level = level if level
        else
          log = create_named_logger(name, level)
        end
        log
      end

      def create_named_logger(name, level)
        StdlibNamedLogger.new(
          name, level ? level : @level, self, root_logger
        ).tap do |log|
          @named_loggers[name] = log
        end
      end

      def root_logger
        unless @root_logger
          @root_logger = ::Logger.new(@log_device)
          @root_logger.level = @level
        end
        @root_logger
      end
    end

    private_constant :StdlibRootLogger
  end
end
