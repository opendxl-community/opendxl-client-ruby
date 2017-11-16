require 'iniparse'

require 'dxlclient/broker'

module DXLClient
  class Config
    DEFAULT_INCOMING_MESSAGE_QUEUE_SIZE = 1000
    DEFAULT_INCOMING_MESSAGE_THREAD_POOL_SIZE = 1
    DEFAULT_MQTT_KEEP_ALIVE_INTERVAL = 30 * 60 # 30 minutes

    DEFAULT_CONNECT_RETRIES = -1 # -1 = infinite
    DEFAULT_RECONNECT_BACK_OFF_MULTIPLIER = 2
    DEFAULT_RECONNECT_DELAY = 1
    DEFAULT_RECONNECT_DELAY_MAX = 60
    DEFAULT_RECONNECT_DELAY_RANDOM = 0.25
    DEFAULT_RECONNECT_WHEN_DISCONNECTED = true

    private_constant :DEFAULT_INCOMING_MESSAGE_QUEUE_SIZE,
                     :DEFAULT_INCOMING_MESSAGE_THREAD_POOL_SIZE,
                     :DEFAULT_MQTT_KEEP_ALIVE_INTERVAL,
                     :DEFAULT_CONNECT_RETRIES,
                     :DEFAULT_RECONNECT_BACK_OFF_MULTIPLIER,
                     :DEFAULT_RECONNECT_DELAY,
                     :DEFAULT_RECONNECT_DELAY_MAX,
                     :DEFAULT_RECONNECT_DELAY_RANDOM,
                     :DEFAULT_RECONNECT_WHEN_DISCONNECTED

    attr_accessor :brokers, :broker_ca_bundle, :client_id,
                  :cert_file, :private_key, :incoming_message_queue_size,
                  :incoming_message_thread_pool_size, :keep_alive_interval,
                  :connect_retries, :reconnect_back_off_multiplier,
                  :reconnect_delay, :reconnect_delay_max,
                  :reconnect_delay_random, :reconnect_when_disconnected

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

      @brokers = brokers || get_brokers_from_config
      @client_id = get_setting('General', 'ClientId') ||
                   UUIDGenerator.generate_id_as_string

      @incoming_message_queue_size = DEFAULT_INCOMING_MESSAGE_QUEUE_SIZE
      @incoming_message_thread_pool_size =
          DEFAULT_INCOMING_MESSAGE_THREAD_POOL_SIZE
      @keep_alive_interval = DEFAULT_MQTT_KEEP_ALIVE_INTERVAL

      @connect_retries = DEFAULT_CONNECT_RETRIES
      @reconnect_back_off_multiplier = DEFAULT_RECONNECT_BACK_OFF_MULTIPLIER
      @reconnect_delay = DEFAULT_RECONNECT_DELAY
      @reconnect_delay_max = DEFAULT_RECONNECT_DELAY_MAX
      @reconnect_delay_random = DEFAULT_RECONNECT_DELAY_RANDOM
      @reconnect_when_disconnected = DEFAULT_RECONNECT_WHEN_DISCONNECTED
    end

    # @return [DXLClient::Config]
    def self.create_dxl_config_from_file(config_file)
      new(config_file: config_file)
    end

    private

    def get_config_model(config_file)
      IniParse.open(config_file) if config_file
    end

    def get_brokers_from_config
      return unless @config_model['Brokers']
      @config_model['Brokers'].lines.collect do |broker_option|
        if broker_option.kind_of?(Array)
          raise ArgumentError,
                format('Broker entry %s defined %d times in config',
                       broker_option.first.key, broker_option.length)
        end

        broker_info = broker_option.value.split(';')
        if broker_info.length < 2
          raise ArgumentError,
                "Missing elements in config broker line: #{broker_info}"
        end

        port = to_port_number(broker_info[0])
        if port
          id = nil
          port = port
          hosts = broker_info[1..-1]
        else
          id = broker_info[0]
          port = to_port_number(broker_info[1])
          unless port
            raise ArgumentError,
                  "Port number not found in config broker line: #{broker_info}"
          end
          hosts = broker_info[2..-1]
        end

        unless hosts
          raise ArgumentError,
                "No hosts found in config broker line: #{broker_info}"
        end

        DXLClient::Broker.new(hosts, id, port)
      end
    end

    def to_port_number(text)
      number = Integer(text)
      number > 0 && number < 65_536 ? number : nil
    rescue ArgumentError
      nil
    end

    def get_setting(section, setting, constructor_param = nil)
      if constructor_param
        constructor_param
      elsif @config_model && !@config_model[section].nil?
        @config_model[section][setting]
      end
    end
  end
end
