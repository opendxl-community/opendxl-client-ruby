# Module under which all of the DXL client functionality resides
module DXLClient
  # Class which represents a DXL message broker.
  class Broker
    attr_reader :hosts, :id, :port

    SSL_PORT = 8883

    def initialize(hosts, id = nil, port = SSL_PORT)
      @hosts = hosts
      @id = id
      @port = port
    end
  end
end
