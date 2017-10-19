require 'securerandom'

module DxlClient
  module UuidGenerator
    def self.generate_id_as_string
      "{#{SecureRandom.uuid}}"
    end
  end
end
