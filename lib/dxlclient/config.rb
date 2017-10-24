require 'iniparse'

require 'dxlclient/broker'

module DXLClient
  class Config
    attr_accessor :broker_ca_bundle, :cert_file, :private_key, :brokers

    def initialize (broker_ca_bundle: nil,
                    cert_file: nil,
                    private_key: nil,
                    brokers: nil,
                    config_file: nil)
      @config_model = config_file ? IniParse.open(config_file) : nil

      @broker_ca_bundle = get_setting('Certs', 'BrokerCertChain',
                                      broker_ca_bundle)
      @cert_file = get_setting('Certs', 'CertFile',
                               cert_file)
      @private_key = get_setting('Certs', 'PrivateKey',
                                 private_key)

      @brokers = get_brokers_from_config()

      @client_id = get_setting('General', 'ClientId')
      unless @client_id
        @client_id = UUIDGenerator.generate_id_as_string
      end
    end

    def self.create_dxl_config_from_file(config_file)
      new(config_file: config_file)
    end

    private

    def get_config_model(config_file)
      if config_file
        IniParse.open(config_file)
      end
    end

    def get_brokers_from_config()
      if @config_model['Brokers']
        @config_model['Brokers'].lines.collect do |broker_option|
          broker_info = broker_option.value.split(';')
          if broker_info.length < 2
            raise ArgumentError(
                      "Missing elements in config broker line: #{broker_info}")
          end

          port = self.class.to_port_number(broker_info[0])
          if port
            id = nil
            port = port
            hosts = broker_info[1..-1]
          else
            id = broker_info[0]
            port = self.class.to_port_number(broker_info[1])
            unless port
              raise ArgumentError(
                        "Port number not found in config broker line: #{broker_info}")
            end
            hosts = broker_info[2..-1]
          end

          unless hosts
            raise ArgumentError(
                      "No hosts found in config broker line: #{broker_info}")
          end

          DXLClient::Broker.new(hosts, id, port)
        end
      end
    end

    def self.to_port_number(text)
      begin
        number = Integer(text)
        if number > 0 && number < 65536
          number
        end
      rescue ArgumentError
        nil
      end
    end

    def get_setting(section, setting, constructor_param=nil)
      if constructor_param
        constructor_param
      elsif @config_model && !@config_model[section].nil?
        @config_model[section][setting]
      end
    end
  end
end
