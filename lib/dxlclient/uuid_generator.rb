require 'securerandom'

module DXLClient
  module UuidGenerator
    def self.generate_id_as_string
      "{#{SecureRandom.uuid}}"
    end
  end

  private_constant :UuidGenerator
end
