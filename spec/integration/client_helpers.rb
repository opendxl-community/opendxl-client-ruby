require 'json'
require 'dxlclient/config'
require 'dxlclient/error'
require 'dxlclient/logger'

module ClientHelpers
  DEFAULT_INCOMING_MESSAGE_THREAD_POOL_SIZE = 1
  DEFAULT_RETRIES = 3
  DEFAULT_TIMEOUT = 5 * 60
  RESPONSE_WAIT = 60
  SERVICE_REGISTRATION_WAIT = 60
  CLIENT_CONFIG_FILE = 'client_config.cfg'.freeze

  def self.client_config_file
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

  # @return [DXLClient::Config]
  def self.client_config(
    max_retries = nil,
    incoming_message_thread_pool_size = nil
  )
    DXLClient::Config.create_dxl_config_from_file(
      client_config_file
    ).tap do |config|
      config.connect_retries = max_retries || DEFAULT_RETRIES
      config.incoming_message_thread_pool_size = \
        incoming_message_thread_pool_size ||
        DEFAULT_INCOMING_MESSAGE_THREAD_POOL_SIZE
    end
  end

  def self.with_integration_client(
    max_retries = nil,
    incoming_message_thread_pool_size = nil,
    &block
  )
    config ||= client_config(max_retries, incoming_message_thread_pool_size)
    if block_given?
      DXLClient::Client.new(config) { |client| block.call(client) }
    else
      DXLClient::Client.new(config)
    end
  end

  def self.while_not_done_and_time_remaining(
    continue_proc, max_wait, start = Time.now
  )
    raise 'No block provided' unless block_given?
    wait_remaining = start - Time.now + max_wait

    continue = continue_proc.call
    while continue && wait_remaining > 0
      yield(wait_remaining)
      wait_remaining = start - Time.now + max_wait
      continue = continue_proc.call
    end

    !continue
  end

  def self.message_payload_as_json(message)
    JSON.parse(message.payload.chomp("\0"))
  end

  def self.with_logged_messages_captured
    raise 'No block provided' unless block_given?
    logged_messages = StringIO.new
    enable_capture_logging(logged_messages)
    yield(logged_messages)
    logged_messages
  ensure
    disable_capture_logging
  end

  def self.enable_capture_logging(log_device)
    root_logger = DXLClient::Logger.root_logger
    root_logger.log_device = log_device
  end

  def self.disable_capture_logging
    enable_capture_logging(STDOUT)
  end
end
