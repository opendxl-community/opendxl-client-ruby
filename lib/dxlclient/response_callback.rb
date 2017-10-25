module DXLClient
  class ResponseCallback
    def on_response(response)
      raise NotImplementedError,
            'No implementation provided for on_response'
    end
  end
end
