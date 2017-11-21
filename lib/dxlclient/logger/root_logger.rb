# Module under which all of the DXL client functionality resides.
module DXLClient
  # Logger to which all code under the DXLClient module logs messages.
  module Logger
    # Subclasses of this base class define a "singleton" root logger.
    class RootLogger
      attr_accessor :level, :log_device

      def initialize
        @level = DXLClient::Logger::INFO
        @log_device = STDOUT
      end

      # @return [DXLClient::Logger::NamedLogger]
      def logger(_name, _level = nil)
        raise NotImplementedError
      end
    end
  end
end
