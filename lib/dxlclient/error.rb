module DXLClient
  module Error
    class DXLError < StandardError
    end

    class IOError < DXLError
    end

    class MalformedBrokerError < DXLError
    end

    class NotConnectedError < IOError
    end

    class WaitTimeoutError < IOError
    end

  end
end
