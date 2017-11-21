require 'dxlclient/config'

describe DXLClient::Config do
  context 'when built from a config file' do
    before(:all) do
      filename = 'dxlclient.config'
      config_file = '#
[General]
ClientId=myclientid

[Certs]
BrokerCertChain=mycertchain.crt
CertFile=mycert.crt
PrivateKey=mykey.key

[Brokers]
broker1=broker1;8883;127.0.0.1
broker2=broker2;9883;localhost;127.0.0.2
broker3=10883;127.0.0.3
'
      RSpec::Mocks.with_temporary_scope do
        allow(File).to receive(:read).with(filename).and_return(config_file)
        @config = DXLClient::Config.create_dxl_config_from_file(filename)
      end
    end

    it 'should return the client id from the file' do
      expect(@config.client_id).to eql('myclientid')
    end

    it 'should return the broker ca bundle from the file' do
      expect(@config.broker_ca_bundle).to eql('mycertchain.crt')
    end

    it 'should return the cert file from the file' do
      expect(@config.cert_file).to eql('mycert.crt')
    end

    it 'should return the private key from the file' do
      expect(@config.private_key).to eql('mykey.key')
    end

    it 'should return the number of brokers from the file' do
      expect(@config.brokers.length).to eql(3)
    end

    it 'should return from the file the broker with one host entry' do
      broker = @config.brokers[0]
      expect(broker.hosts).to eql(['127.0.0.1'])
      expect(broker.id).to eql('broker1')
      expect(broker.port).to eql(8883)
    end

    it 'should return from the file the broker with two host entries' do
      broker = @config.brokers[1]
      expect(broker.hosts).to eql(%w[localhost 127.0.0.2])
      expect(broker.id).to eql('broker2')
      expect(broker.port).to eql(9883)
    end

    it 'should return from the file the broker with no id' do
      broker = @config.brokers[2]
      expect(broker.hosts).to eql(['127.0.0.3'])
      expect(broker.id).to be_nil
      expect(broker.port).to eql(10_883)
    end
  end

  context 'when specifying settings during construction' do
    before(:all) do
      @config = DXLClient::Config.new(
        'mycertchain.crt',
        'mycert.crt',
        'mykey.key',
        [
          DXLClient::Broker.new(['127.0.0.1'], 'broker1'),
          DXLClient::Broker.new(%w[localhost 127.0.0.2], 'broker2', 9883),
          DXLClient::Broker.new(['127.0.0.3'], nil, 10_883)
        ]
      )
    end

    it 'should return a uuid-formatted client id' do
      expect(@config.client_id).to \
        match(/{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/)
    end

    it 'should return the specified broker ca bundle' do
      expect(@config.broker_ca_bundle).to eql('mycertchain.crt')
    end

    it 'should return the specified cert file' do
      expect(@config.cert_file).to eql('mycert.crt')
    end

    it 'should return the specified private key' do
      expect(@config.private_key).to eql('mykey.key')
    end

    it 'should return the specified number of brokers' do
      expect(@config.brokers.length).to eql(3)
    end

    it 'should return the specified info for a broker with one host entry' do
      broker = @config.brokers[0]
      expect(broker.hosts).to eql(['127.0.0.1'])
      expect(broker.id).to eql('broker1')
      expect(broker.port).to eql(8883)
    end

    it 'should return the specified info for a broker with two host entries' do
      broker = @config.brokers[1]
      expect(broker.hosts).to eql(%w[localhost 127.0.0.2])
      expect(broker.id).to eql('broker2')
      expect(broker.port).to eql(9883)
    end

    it 'should return the specified info for a broker with no id' do
      broker = @config.brokers[2]
      expect(broker.hosts).to eql(['127.0.0.3'])
      expect(broker.id).to be_nil
      expect(broker.port).to eql(10_883)
    end
  end
end
