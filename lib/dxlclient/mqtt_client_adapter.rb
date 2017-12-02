require 'dxlclient/error'
require 'dxlclient/logger'
require 'dxlclient/mqtt_client'
require 'dxlclient/util'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Wrapper for the underlying MQTT::Client class which exposes the subset
  # of client functionality that DXL needs and which translates MQTT exceptions
  # into exceptions under the DXLClient namespace.
  class MQTTClientAdapter
    MQTT_QOS = 0
    MQTT_VERSION = '3.1.1'.freeze

    private_constant :MQTT_QOS, :MQTT_VERSION

    # rubocop: disable AbcSize, MethodLength

    # @param config [DXLClient::Config]
    def initialize(config)
      @config = config
      @logger = DXLClient::Logger.logger(self.class.name)

      @mqtt_client = MQTTClient.new(client_id: config.client_id,
                                    version: MQTT_VERSION,
                                    clean_session: true,
                                    ssl: true,
                                    keep_alive: config.keep_alive_interval)
      @mqtt_client.cert_file = config.cert_file
      @mqtt_client.key_file = config.private_key
      @mqtt_client.ca_file = config.broker_ca_bundle
      @mqtt_client.on_message(method(:on_message))
      @on_message_lock = Mutex.new
      @on_message_callbacks = Set.new
    end

    # rubocop: enable AbcSize, MethodLength

    # Register a callback to be invoked with the content of a published message
    def add_message_callback(callback)
      @on_message_lock.synchronize do
        @on_message_callbacks.add(callback)
      end
    end

    def connect
      exception_prefix =
        "Failed to connect to #{@mqtt_client.host}:#{@mqtt_client.port}"
      recast_exception(exception_prefix) { @mqtt_client.connect }
    end

    def connected?
      @mqtt_client.connected?
    end

    def disconnect
      recast_exception('Failed to disconnect') { @mqtt_client.disconnect }
    end

    def host
      @mqtt_client.host
    end

    def host=(value)
      @mqtt_client.host = value
    end

    def publish(topic, payload = '')
      recast_exception('Failed to publish message') do
        @mqtt_client.publish(topic, payload, false, MQTT_QOS)
      end
    end

    def port
      @mqtt_client.port
    end

    def port=(value)
      @mqtt_client.port = value
    end

    def subscribe(*topics)
      recast_exception('Failed to subscribe to topics') do
        @mqtt_client.subscribe(topics)
      end
    end

    def unsubscribe(*topics)
      recast_exception('Failed to unsubscribe from topics') do
        @mqtt_client.unsubscribe(topics)
      end
    end

    private

    def recast_exception(message_prefix = nil)
      yield
    rescue MQTT::NotConnectedException => e
      raise_exception(DXLClient::Error::NotConnectedError, e, message_prefix)
    rescue Timeout::Error => e
      raise_exception(DXLClient::Error::WaitTimeoutError, e, message_prefix)
    rescue MQTT::Exception, Errno::ECONNREFUSED, Errno::ECONNABORTED,
           Errno::ECONNRESET, Errno::EPIPE, IOError,
           OpenSSL::SSL::SSLError, SocketError => e
      raise_exception(DXLClient::Error::IOError, e, message_prefix)
    end

    def raise_exception(new_exception, original_exception, message_prefix)
      raise new_exception,
            exception_message(original_exception, message_prefix)
    end

    def exception_message(exception, message_prefix)
      message = Util.exception_message(exception)
      if message_prefix.nil? || message_prefix.empty?
        message
      else
        "#{message_prefix}: #{message}"
      end
    end

    def on_message(packet)
      @on_message_lock.synchronize do
        @on_message_callbacks.each do |callback|
          begin
            callback.call(packet)
          rescue StandardError => e
            @logger.exception(e, 'Error raised by message callback')
          end
        end
      end
    end
  end

  private_constant :MQTTClientAdapter
end
