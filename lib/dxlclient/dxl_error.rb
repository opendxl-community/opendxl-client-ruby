module DXLClient
  class DXLError < StandardError
  end

  class ShutdownError < DXLError
  end
end
