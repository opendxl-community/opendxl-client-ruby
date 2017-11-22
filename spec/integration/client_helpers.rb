require 'dxlclient/error'

module ClientHelpers
  DEFAULT_TIMEOUT = 5 * 60
  RESPONSE_WAIT = 60

  def self.client_config
    config = ['client_config.cfg.local', 'client_config.cfg'].find do |config|
      File.exist?(config)
    end
    unless config
      raise DXLClient::Error::DXLError, 'Unable to locate client config'
    end
    config
  end

  def self.with_integration_client(&block)
    config = DXLClient::Config.create_dxl_config_from_file(client_config)
    if block_given?
      DXLClient::Client.new(config) { |client| block.call(client) }
    else
      DXLClient::Client.new(config)
    end
  end
end
