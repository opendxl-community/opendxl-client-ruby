module DXLClient
  module Error
    class DXLError < StandardError
    end

    class ShutdownError < DXLError
    end
  end
end
