require 'iniparse'

require 'dxlclient/broker'
require 'dxlclient/util'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # rubocop: disable ClassLength

  # The Data Exchange Layer (DXL) client configuration contains the information
  # necessary to connect a {DXLClient::Client} to the DXL fabric.
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

    # rubocop: disable AbcSize, MethodLength

    # Constructor
    def initialize(broker_ca_bundle = nil,
                   cert_file = nil,
                   private_key = nil,
                   brokers = nil,
                   config_file = nil)
      @config_model = config_model_from_file(config_file)

      @broker_ca_bundle = get_setting('Certs', 'BrokerCertChain',
                                      broker_ca_bundle)
      @cert_file = get_setting('Certs', 'CertFile', cert_file)
      @private_key = get_setting('Certs', 'PrivateKey', private_key)

      @brokers = brokers || brokers_from_config_section || []
      @client_id = get_setting('General', 'ClientId', nil,
                               Util.generate_id_as_string)

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
    # rubocop: enable AbcSize, MethodLength

    # @return [DXLClient::Config]
    def self.create_dxl_config_from_file(config_file)
      new(nil, nil, nil, nil, config_file)
    end

    private

    def config_model_from_file(config_file)
      config_file ? IniParse.open(config_file) : nil
    end

    def brokers_from_config_section
      return unless @config_model && @config_model['Brokers']
      @config_model['Brokers'].lines.collect do |broker_option|
        if broker_option.is_a?(Array)
          raise ArgumentError,
                format('Broker entry %s defined %d times in config',
                       broker_option.first.key, broker_option.length)
        end
        broker_from_config_line(broker_option.value.split(';'))
      end
    end

    def broker_from_config_line(broker_info)
      if broker_info.length < 2
        raise ArgumentError,
              "Missing elements in config broker line: #{broker_info}"
      end
      id, hosts, port = broker_config_line_elements(broker_info)
      unless hosts
        raise ArgumentError,
              "No hosts found in config broker line: #{broker_info}"
      end

      DXLClient::Broker.new(hosts, id, port)
    end

    def broker_config_line_elements(broker_info)
      port_position = to_port_number(broker_info[0]) ? 0 : 1
      id = port_position.zero? ? nil : broker_info[0]
      port = to_port_number(broker_info[port_position])
      unless port
        raise ArgumentError,
              format('Invalid broker port number found in config: %s',
                     broker_info[port_position])
      end
      hosts = broker_info[(port_position + 1)..-1]
      [id, hosts, port]
    end

    def to_port_number(text)
      number = Integer(text)
      number > 0 && number < 65_536 ? number : nil
    rescue ArgumentError
      nil
    end

    def get_setting(section, setting, constructor_param = nil,
                    default_value = nil)
      if constructor_param
        constructor_param
      elsif @config_model && !@config_model[section].nil?
        @config_model[section][setting]
      else
        default_value
      end
    end
  end
end
