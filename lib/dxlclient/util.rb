require 'securerandom'

# Module under which all of the DXL client functionality resides.
module DXLClient
  # Module for generating Universally Unique Identifiers (UUID).
  module Util
    def self.generate_id_as_string
      "{#{SecureRandom.uuid}}"
    end

    def self.to_port_number(text)
      number = port_as_integer(text)
      if number > 0 && number < 65_536
        number
      else
        raise ArgumentError,
              "Port number #{text} not in valid range (1-65535)"
      end
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

    def self.port_as_integer(text)
      Integer(text)
    rescue ArgumentError => e
      raise ArgumentError,
            "Error parsing port number: #{e.message}"
    end

    private_class_method :port_as_integer
  end
end
