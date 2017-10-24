module DXLClient
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
