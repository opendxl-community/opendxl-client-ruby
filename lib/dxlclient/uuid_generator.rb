require 'securerandom'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Module for generating Universally Unique Identifiers (UUID).
  module UUIDGenerator
    def self.generate_id_as_string
      "{#{SecureRandom.uuid}}"
    end
  end

  private_constant :UUIDGenerator
end
