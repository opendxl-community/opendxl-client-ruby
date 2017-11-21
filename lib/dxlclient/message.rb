require 'dxlclient/message/error_response'
require 'dxlclient/message/event'
require 'dxlclient/message/message'
require 'dxlclient/message/request'
require 'dxlclient/message/response'

module DXLClient
  module Message
    MESSAGE_TYPE_REQUEST = 0
    MESSAGE_TYPE_RESPONSE = 1
    MESSAGE_TYPE_EVENT = 2
    MESSAGE_TYPE_ERROR = 3
  end
end
