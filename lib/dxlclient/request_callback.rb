module DXLClient
  class RequestCallback
    def on_request(request)
      raise NotImplementedError,
            'No implementation provided for on_request'
    end
  end
end
