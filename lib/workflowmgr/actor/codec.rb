##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  class Actor
    ##########################################
    #
    # Module Codec
    #
    # Turns the values rocoto passes around into something JSON can carry,
    # and back again. JSON knows nothing of Symbols, Times, or hashes keyed
    # by anything but strings, and left to itself it converts them into
    # strings without complaint -- which is worse than refusing, since the
    # caller then gets a plausible wrong answer rather than an error. A
    # range of cycles asked for as {start:, end:} arrives as {"start"=>...},
    # reads back as nil, and quietly becomes "every cycle ever recorded".
    #
    # Anything this cannot carry raises instead of degrading, and the only
    # classes it will rebuild are those that opt in by defining from_wire.
    # A message can therefore never name an arbitrary class to instantiate,
    # which is the property that made JSON preferable to Marshal here.
    #
    ##########################################
    module Codec
      require 'json'

      # A tagged value is a two-key object, {"$" => type, "v" => payload}.
      # Any other hash is ordinary data.
      TAG = "$".freeze
      VALUE = "v".freeze

      # JSON itself gives up beyond 100 levels, and each level here costs
      # about three of its own once a hash has to be escaped into pairs.
      # Stopping first means an error that names the real problem rather
      # than JSON's, and it is also what keeps a structure that contains
      # itself from recursing until the stack gives out -- which would take
      # the whole actor down rather than failing the one call.
      MAX_DEPTH = 32

      # The encodings JSON itself speaks. Held as a constant because every
      # string that crosses to or from an actor is checked against it.
      TEXT_ENCODINGS = [Encoding::US_ASCII, Encoding::UTF_8].freeze

      class Unsupported < StandardError; end

      class << self
        ##########################################
        #
        # encode
        #
        ##########################################
        def encode(value, depth = 0)
          too_deep! if depth > MAX_DEPTH

          case value
          when nil, true, false, Integer then value
          when Float then encode_float(value)
          when String then encode_string(value)
          when Symbol then tagged("Symbol", value.to_s)
          when Time then tagged("Time", [value.tv_sec, value.tv_nsec, zone_of(value)])
          when Array then value.map { |element| encode(element, depth + 1) }
          when Hash then encode_hash(value, depth)
          else encode_object(value, depth)
          end
        end

        ##########################################
        #
        # decode
        #
        ##########################################
        def decode(value, depth = 0)
          too_deep! if depth > MAX_DEPTH

          case value
          when Array then value.map { |element| decode(element, depth + 1) }
          when Hash then decode_hash(value, depth)
          else value
          end
        end

        private

        def tagged(type, payload)
          { TAG => type, VALUE => payload }
        end

        def tagged?(hash)
          hash.size == 2 && hash.key?(TAG) && hash.key?(VALUE)
        end

        def too_deep!
          raise Unsupported, "a structure nested deeper than #{MAX_DEPTH} levels cannot be carried to or " \
                             "from an actor; one that contains itself never can"
        end

        # JSON has no way to write these, so they would fail later, further
        # from the value that caused it.
        def encode_float(value)
          return value if value.finite?

          raise Unsupported, "#{value} cannot be carried to or from an actor: JSON has no way to write it"
        end

        # Bytes JSON cannot write are carried as base64 rather than refused.
        # File contents and command output are not always text -- a workflow
        # document with one latin-1 byte in it is still a workflow document
        # -- and DRb carried them before this. Base64 is faithful, not a
        # degradation: what arrives is byte-for-byte what was sent.
        def encode_string(value)
          return value if carryable_as_text?(value)

          tagged("Bytes", [value.encoding.name, [value].pack("m0")])
        end

        # Only encodings JSON itself speaks travel as plain text, and only
        # when the bytes are really what they claim to be. A string in some
        # other encoding would otherwise come back claiming to be UTF-8,
        # which is a small lie the caller cannot see.
        #
        # The validity check matters as much for US-ASCII as for UTF-8, and
        # is easier to overlook there: File.read tags what it returns with
        # the process's default external encoding, which on a machine with
        # no locale set is US-ASCII. A workflow document holding one latin-1
        # byte therefore arrives claiming to be ASCII, and taking that claim
        # at its word hands JSON a string it cannot write.
        #
        # US-ASCII is also the one label not preserved: a valid US-ASCII
        # string arrives as UTF-8. Its bytes are unchanged and every
        # US-ASCII string is valid UTF-8, so nothing can read differently
        # for it -- while keeping the label would mean base64-ing the
        # contents of every file read where no locale is set, which is all
        # of them.
        def carryable_as_text?(value)
          return false unless value.valid_encoding?

          TEXT_ENCODINGS.include?(value.encoding)
        end

        # A hash with nothing but string keys is carried as itself, which
        # keeps the common case cheap. Anything else -- symbol keys, Time
        # keys, or a hash that happens to contain our tag -- is carried as a
        # list of pairs, so ordinary data can never be mistaken for a tag on
        # the way back.
        def encode_hash(hash, depth)
          # A key that cannot travel as plain text has to go the long way
          # round: JSON object keys are strings, so a tagged one could not
          # be a key at all.
          if hash.keys.all? { |key| key.is_a?(String) && carryable_as_text?(key) } && !hash.key?(TAG)
            hash.transform_values { |value| encode(value, depth + 1) }
          else
            tagged("Hash", hash.map { |key, value| [encode(key, depth + 1), encode(value, depth + 1)] })
          end
        end

        def decode_hash(hash, depth)
          return hash.transform_values { |value| decode(value, depth + 1) } unless tagged?(hash)

          case hash[TAG]
          when "Symbol" then hash[VALUE].to_sym
          when "Bytes" then decode_bytes(hash[VALUE])
          when "Time" then decode_time(hash[VALUE])
          when "Hash" then decode_pairs(hash[VALUE], depth)
          else decode_object(hash[TAG], hash[VALUE], depth)
          end
        end

        def decode_pairs(payload, depth)
          unless payload.is_a?(Array) && payload.all? { |pair| pair.is_a?(Array) && pair.size == 2 }
            raise Unsupported, "a hash arrived in a message, but not as the pairs it should have been written as"
          end

          payload.to_h { |key, value| [decode(key, depth + 1), decode(value, depth + 1)] }
        end

        # UTC, or the offset the original Time was written in. A bare "is it
        # UTC" flag would be enough for equality, since that compares
        # instants, but not for strftime -- and Cycle prints itself with
        # strftime, so an offset lost here shows up as the wrong hour.
        def zone_of(time)
          time.utc? ? "utc" : time.utc_offset
        end

        # Checked rather than trusted, like every other tag: what arrives
        # here was written by another process, and a malformed payload
        # should name itself rather than fail as a NoMethodError further on.
        def decode_bytes(payload)
          unless payload.is_a?(Array) && payload.size == 2 && payload.all?(String)
            raise Unsupported, "bytes arrived in a message, but not as the encoding and data they should have been"
          end

          encoding, data = payload
          begin
            data.unpack1("m0").force_encoding(Encoding.find(encoding))
          rescue ArgumentError => e
            raise Unsupported, "bytes arrived in a message but could not be rebuilt: #{e.message}"
          end
        end

        def decode_time(payload)
          seconds, nanoseconds, zone = payload
          time = Time.at(seconds, nanoseconds, :nanosecond)
          return time.utc if zone == "utc"

          zone.nil? ? time : time.localtime(zone)
        end

        def encode_object(object, depth)
          unless object.respond_to?(:to_wire) && object.class.respond_to?(:from_wire)
            raise Unsupported, "a #{object.class} cannot be carried to or from an actor; " \
                               "define to_wire and self.from_wire on it to say how"
          end

          tagged(object.class.name, encode(object.to_wire, depth + 1))
        end

        def decode_object(type, payload, depth)
          klass = begin
            Object.const_get(type.to_s)
          rescue NameError, TypeError
            nil
          end

          unless klass.is_a?(Class) && klass.respond_to?(:from_wire)
            raise Unsupported, "#{type.inspect} arrived in a message, but no class of that name here defines " \
                               "from_wire; it may simply not be loaded in this process"
          end

          klass.from_wire(decode(payload, depth + 1))
        end
      end
    end
  end
end
