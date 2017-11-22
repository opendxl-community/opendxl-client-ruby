require 'dxlclient/error'

module ClientHelpers
  DEFAULT_INCOMING_MESSAGE_THREAD_POOL_SIZE = 1
  DEFAULT_RETRIES = 3
  DEFAULT_TIMEOUT = 5 * 60
  RESPONSE_WAIT = 60
  CLIENT_CONFIG_FILE = 'client_config.cfg'.freeze

  def self.client_config
    config_dirs = [Dir.home, File.dirname(__FILE__)]
    config = config_dirs.find do |config_dir|
      File.exist?(File.join(config_dir, CLIENT_CONFIG_FILE))
    end
    unless config
      raise DXLClient::Error::DXLError,
            "Unable to locate client config at #{File.join(config_dirs[-1],
                                                           CLIENT_CONFIG_FILE)}"
    end
    File.join(config, CLIENT_CONFIG_FILE)
  end

  def self.with_integration_client(
    max_retries = DEFAULT_RETRIES,
    incoming_message_thread_pool_size = \
      DEFAULT_INCOMING_MESSAGE_THREAD_POOL_SIZE,
    &block
  )
    config = DXLClient::Config.create_dxl_config_from_file(client_config)
    config.incoming_message_thread_pool_size = \
      incoming_message_thread_pool_size
    config.connect_retries = max_retries
    if block_given?
      DXLClient::Client.new(config) { |client| block.call(client) }
    else
      DXLClient::Client.new(config)
    end
  end
end
