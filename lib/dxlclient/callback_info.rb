# Module under which all of the DXL client functionality resides
module DXLClient
  # Wrapper for a DXL message callback which captures the callback `Proc`
  # along with related information for the callback - for example, whether
  # or not a subscription is registered along with the callback.
  class CallbackInfo
    attr_reader :callback

    def initialize(callback, subscribe = true)
      @callback = callback
      @subscribe = subscribe
    end

    def subscribe?
      @subscribe
    end
  end

  private_constant :CallbackInfo
end
