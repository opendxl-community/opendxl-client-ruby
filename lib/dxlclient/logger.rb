require 'logger'
require 'dxlclient/logger/stdlib_root_logger'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Logger to which all code under the DXLClient module logs messages.
  module Logger
    DEBUG = ::Logger::DEBUG
    ERROR = ::Logger::ERROR
    INFO = ::Logger::INFO
    WARN = ::Logger::WARN

    @root_logger = nil
    @root_logger_lock = Mutex.new

    # @return [DXLClient::Logger::NamedLogger]
    def self.logger(name, level = nil)
      root_logger.logger(name, level)
    end

    # @return [DXLClient::Logger::RootLogger]
    def self.root_logger
      @root_logger_lock.synchronize { @root_logger ||= StdlibRootLogger.new }
    end

    def self.root_logger=(logger)
      @root_logger_lock.synchronize { @root_logger = logger }
    end
  end
end
