# Module under which all of the DXL client functionality resides.
module DXLClient
  module Callback
    # Subclasses of this base class are used to receive {DXLClient::Request}
    # messages.
    class RequestCallback
      def on_request(_request)
        raise NotImplementedError,
              'No implementation provided for on_request'
      end
    end
  end
end
