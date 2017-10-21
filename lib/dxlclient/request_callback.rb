module DXLClient
  class RequestCallback
    def on_request(request)
      raise NotImplementedError.new("No implementation provided for on_request")
    end
  end
end
