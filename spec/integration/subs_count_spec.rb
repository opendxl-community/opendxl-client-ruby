require 'dxlclient/client'
require 'dxlclient/message/request'
require 'dxlclient/service_registration_info'
require 'integration/client_helpers'
require 'dxlclient/util'

DXLClient::Logger.root_logger.level = DXLClient::Logger::ERROR

describe 'broker subs count', :integration do
  max_wait = 5

  def create_subs_request(topic)
    DXLClient::Message::Request.new(
      '/mcafee/service/dxl/broker/subs'
    ).tap { |request| request.payload = JSON.dump('topic' => topic) }
  end

  it 'should return expected values for topic subscriptions' do
    ClientHelpers.with_integration_client do |client|
      random1 = DXLClient::Util.generate_id_as_string
      random2 = DXLClient::Util.generate_id_as_string
      topic1 = \
        "subs_count_spec/foo/#{random1}/#{random2}"
      topic2 = "subs_count_spec/bar/#{random2}"

      client.connect
      extra_clients = Array.new(6) do
        ClientHelpers.with_integration_client.tap(&:connect)
      end

      begin
        extra_clients[0].subscribe(topic1)
        extra_clients[1].subscribe(topic1)
        extra_clients[2].subscribe(topic1)
        extra_clients[3].subscribe("subs_count_spec/foo/#{random1}/#")
        extra_clients[4].subscribe("subs_count_spec/+/#{random1}/#")

        extra_clients[1].subscribe(topic2)
        extra_clients[2].subscribe(topic2)
        extra_clients[5].subscribe('#')

        response = client.sync_request(create_subs_request(topic1), max_wait)
        expect(
          ClientHelpers.message_payload_as_json(response)['count']
        ).to eq(6)

        response = client.sync_request(create_subs_request(topic2), max_wait)
        expect(
          ClientHelpers.message_payload_as_json(response)['count']
        ).to eq(3)
      ensure
        extra_clients.each(&:destroy)
      end
    end
  end
end
