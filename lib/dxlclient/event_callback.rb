# Module under which all of the DXL client functionality resides.
module DXLClient
  # Subclasses of this base class are used to receive {DXLClient::Event}
  # messages.
  class EventCallback
    def on_event(_event)
      raise NotImplementedError
    end
  end
end
