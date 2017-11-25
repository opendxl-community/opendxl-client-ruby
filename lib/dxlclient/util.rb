require 'securerandom'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Module for generating Universally Unique Identifiers (UUID).
  module Util
    def self.generate_id_as_string
      "{#{SecureRandom.uuid}}"
    end

    def self.current_thread_name(name)
      current_thread = Thread.current
      if current_thread.respond_to?(:name)
        current_thread.name = name
      else
        current_thread[:name] = name
      end
    end
  end
end
