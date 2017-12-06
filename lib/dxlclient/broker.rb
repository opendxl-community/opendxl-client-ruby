require 'dxlclient/error'
require 'dxlclient/util'

# Module under which all of the DXL client functionality resides
module DXLClient
  # Class which represents a DXL message broker.
  class Broker
    attr_reader :hosts, :id, :port

    SSL_PORT = 8883
    SSL_PROTOCOL = 'ssl'.freeze

    private_constant :SSL_PORT, :SSL_PROTOCOL

    def initialize(hosts, id = nil, port = SSL_PORT)
      @hosts = [*hosts]
      @id = id
      @port = get_port_as_integer(port)
    end

    def self.parse(broker_url)
      host_name = broker_url
      protocol = SSL_PROTOCOL

      url_elements = broker_url.split('://')
      protocol, host_name = url_elements if url_elements.length == 2
      host_name, port = parse_host_name_and_port(host_name)

      unless protocol == SSL_PROTOCOL
        raise DXLClient::Error::MalformedBrokerError,
              "Unknown protocol: #{protocol}"
      end

      new(host_name, DXLClient::Util.generate_id_as_string, port)
    end

    private

    def get_port_as_integer(text)
      DXLClient::Util.to_port_number(text)
    rescue ArgumentError => e
      raise DXLClient::Error::MalformedBrokerError,
            "Invalid broker port number: #{e.message}"
    end

    def self.parse_host_name_and_port(host_name)
      port = SSL_PORT

      if host_name[-1] != ']'
        left_part, _, right_part = host_name.rpartition(':')
        unless left_part.empty?
          host_name = left_part
          port = right_part
        end
      end

      [host_name.delete('[]'), port]
    end

    private_class_method :parse_host_name_and_port
  end
end
