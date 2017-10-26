module DXLClient
  class CallbackInfo
    attr_reader :callback

    def initialize(callback, subscribe=true)
      @callback = callback
      @subscribe = subscribe
    end

    def subscribe?
      @subscribe
    end
  end

  # private_constant :CallbackInfo
end
