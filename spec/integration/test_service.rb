require 'dxlclient/client'
require 'dxlclient/callback/request_callback'
require 'dxlclient/message/error_response'

class TestService < DXLClient::Callback::RequestCallback
  attr_accessor :return_error, :error_code, :error_message

  # @param client [DXLClient::Client]
  def initialize(client)
    @client = client
    @error_code = 99
    @error_message = 'Error'
    @return_error = false
  end

  # @param request [DXLClient::Message::Request]
  def on_request(request)
    response =
      if @return_error
        DXLClient::Message::ErrorResponse.new(request,
                                              @error_code,
                                              @error_message)
      else
        DXLClient::Message::Response.new(request)
      end
    @client.send_response(response)
  end
end
