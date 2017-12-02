require 'securerandom'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Module for generating Universally Unique Identifiers (UUID).
  module Util
    def self.generate_id_as_string
      "{#{SecureRandom.uuid}}"
    end

    def self.current_thread_name(name)
      Thread.current.tap do |current_thread|
        if current_thread.respond_to?(:name)
          current_thread.name = name
        else
          current_thread[:name] = name
        end
      end
      name
    end

    def self.exception_message(exception)
      if exception.message == exception.class.to_s
        exception.message
      else
        "#{exception.message} (#{exception.class})"
      end
    end
  end
end
