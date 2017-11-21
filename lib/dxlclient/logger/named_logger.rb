# Module under which all of the DXL client functionality resides.
module DXLClient
  # Logger to which all code under the DXLClient module logs messages.
  module Logger
    # Subclasses of this base class define a named logger. The logger's name
    # can be written out as part of each logged message.
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

      def debugf(*)
        raise NotImplementedError
      end

      def error(_message)
        raise NotImplementedError
      end

      def errorf(*)
        raise NotImplementedError
      end

      def info(_message)
        raise NotImplementedError
      end

      def infof(*)
        raise NotImplementedError
      end

      def warn(_message)
        raise NotImplementedError
      end

      def warnf(*)
        raise NotImplementedError
      end

      def exception(_exception, _message = nil)
        raise NotImplementedError
      end
    end
  end
end
