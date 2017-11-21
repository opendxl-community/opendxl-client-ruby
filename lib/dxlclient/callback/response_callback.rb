# Module under which all of the DXL client functionality resides.
module DXLClient
  module Callback
    # Subclasses of this base class are used to receive {DXLClient::Response}
    # messages.
    class ResponseCallback
      def on_response(_response)
        raise NotImplementedError,
              'No implementation provided for on_response'
      end
    end
  end
end
