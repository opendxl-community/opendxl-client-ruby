module DXLClient
  class EventCallback
    def on_event(event)
      raise NotImplementedError.new('No implementation provided for on_event')
    end
  end
end
