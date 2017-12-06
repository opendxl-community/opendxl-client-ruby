require 'dxlclient/broker'
require 'dxlclient/error'

describe DXLClient::Broker do
  context '.parse' do
    it 'should construct a broker from a host name' do
      broker = DXLClient::Broker.parse('mybroker')
      expect(broker).to be_an_instance_of(DXLClient::Broker)
      expect(broker.hosts).to eql(['mybroker'])
      expect(broker.port).to eql(8883)
    end

    it 'should construct a broker from a host name and port' do
      broker = DXLClient::Broker.parse('mybroker:8993')
      expect(broker).to be_an_instance_of(DXLClient::Broker)
      expect(broker.hosts).to eql(['mybroker'])
      expect(broker.port).to eql(8993)
    end

    it 'should construct a broker from a protocol and host name' do
      broker = DXLClient::Broker.parse('ssl://mybroker')
      expect(broker).to be_an_instance_of(DXLClient::Broker)
      expect(broker.hosts).to eql(['mybroker'])
      expect(broker.port).to eql(8883)
    end

    it 'should construct a broker from a protocol, host name, and port' do
      broker = DXLClient::Broker.parse('ssl://mybroker:8993')
      expect(broker).to be_an_instance_of(DXLClient::Broker)
      expect(broker.hosts).to eql(['mybroker'])
      expect(broker.port).to eql(8993)
    end

    it 'should generate an id for a broker instance' do
      broker = DXLClient::Broker.parse('mybroker')
      expect(broker).to be_an_instance_of(DXLClient::Broker)
      expect(broker.id).to_not be_empty
    end

    context 'with an IPv6-based hostname and no port' do
      it 'should construct a broker with brackets stripped' do
        broker = DXLClient::Broker.parse('[ff02::1]')
        expect(broker).to be_an_instance_of(DXLClient::Broker)
        expect(broker.hosts).to eql(['ff02::1'])
        expect(broker.port).to eql(8883)
      end
    end

    context 'with an IPv6-based hostname and a port' do
      it 'should construct a broker with brackets stripped' do
        broker = DXLClient::Broker.parse('[ff02::1]:8993')
        expect(broker).to be_an_instance_of(DXLClient::Broker)
        expect(broker.hosts).to eql(['ff02::1'])
        expect(broker.port).to eql(8993)
      end
    end
  end
end

