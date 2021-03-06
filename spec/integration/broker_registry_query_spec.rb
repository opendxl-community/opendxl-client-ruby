require 'dxlclient/client'
require 'dxlclient/logger'
require 'dxlclient/message/request'
require 'dxlclient/message/response'
require 'integration/client_helpers'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'broker registry query', :integration do
  it 'should return a proper response' do
    ClientHelpers.with_integration_client(0) do |client|
      client.connect

      topic = '/mcafee/service/dxl/brokerregistry/query'
      request = DXLClient::Message::Request.new(topic)
      request.payload = '{}'

      response = client.sync_request(request)

      expect(response).to be_an_instance_of(DXLClient::Message::Response)
      expect(response.source_broker_id).to_not be_empty
      expect(response.source_client_id).to_not be_empty

      response_payload_as_hash = \
        ClientHelpers.message_payload_as_json(response)
      expect(response_payload_as_hash).to be_an_instance_of(Hash)
    end
  end
end
