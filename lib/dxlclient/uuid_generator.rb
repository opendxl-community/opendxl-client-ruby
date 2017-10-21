require 'securerandom'

module DXLClient
  module UUIDGenerator
    def self.generate_id_as_string
      "{#{SecureRandom.uuid}}"
    end
  end

  private_constant :UUIDGenerator
end
