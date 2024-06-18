
module Pocolog
    class Logfiles
        def self.write_prologue(to_io, big_endian = nil)
            to_io.write(MAGIC)
            if big_endian.nil?
                big_endian = Pocolog.big_endian?
            end
            to_io.write(*[FORMAT_VERSION, big_endian ? 1 : 0].pack('xVV'))
        end

        # Converts a version 1 logfile. Modifications:
        # * no prologue
        # * no compressed flag on data blocks
        # * time was written as [type, sec, usec, padding], with each
        #   field a 32-bit integer
        def self.from_version_1(from, to_io, big_endian)
            write_prologue(to_io, big_endian)
            from_io = from.io

            buffer = ""
            uncompressed = [0].pack('C')
            while (block_info = from.read_next_block_header)
                if block_info.type == STREAM_BLOCK
                    payload = from.block_stream.read_payload
                    Logfiles.write_block(to_io, block_info.kind, block_info.stream_index, payload)
                elsif block_info.type == CONTROL_BLOCK
                    # remove the fields in time structure
                    to_io.write([block_info.type, block_info.stream_index, block_info.payload_size - 16].pack('CxvV'))
                    from_io.seek(4, IO::SEEK_CUR)
                    to_io.write(from_io.read(8))
                    from_io.seek(4, IO::SEEK_CUR)
                    to_io.write(from_io.read(1))
                    from_io.seek(4, IO::SEEK_CUR)
                    to_io.write(from_io.read(8))
                else
                    size_offset = - 16 + 1

                    to_io.write([block_info.type, block_info.stream_index, block_info.payload_size + size_offset].pack('CxvV'))
                    from_io.seek(4, IO::SEEK_CUR)
                    to_io.write(from_io.read(8))
                    from_io.seek(8, IO::SEEK_CUR)
                    to_io.write(from_io.read(8))
                    from_io.seek(4, IO::SEEK_CUR)
                    to_io.write(from_io.read(4))
                    to_io.write(uncompressed)
                    from_io.read(block_info.payload_size - (DATA_HEADER_SIZE - size_offset), buffer)
                    to_io.write(buffer)
                end
            end
        end

        def self.to_new_format(from_io, to_io, big_endian = nil)
            from = BlockStream.new(from_io)
            from.read_prologue

        rescue MissingPrologue
            # This is format version 1. Need either --little-endian or --big-endian
            if big_endian.nil?
                raise "#{from_io.path} looks like a v1 log file. You must specify either --little-endian or --big-endian"
            end
            puts "#{from_io.path}: format v1 in #{big_endian ? "big endian" : "little endian"}"
            from_version_1(from, to_io, big_endian)

        rescue ObsoleteVersion
        end
        
        def self.compress(from_io, to_io)
            from = Logfiles.new(from_io)
            write_prologue(to_io, from.endian_swap ^ Pocolog.big_endian?)

            from.each_block_header do |block_info|
                if block_info.kind == DATA_BLOCK
                    data_header = from.block_stream.read_data_block_header
                    payload = from.block_stream.read_payload

                    compressed = data_header.compressed?

                    if !compressed
                        if payload.size > Logfiles::COMPRESSION_MIN_SIZE
                            payload2 = Zlib::Deflate.deflate(payload)
                            if payload2.size < payload.size
                                compressed = true
                                payload = payload2
                            end
                        end
                    end

                    Logfiles.write_data_block(to_io, block_info.stream_index,
                                              data_header.rt_time,
                                              data_header.lg_time,
                                              compressed ? 1 : 0,
                                              payload)
                else
                    payload = from.block_stream.read_payload
                    Logfiles.write_block(to_io, block_info.kind, block_info.stream_index, payload)
                end
            end
        end

        def self.rename_streams(from_io, to_io, mappings)
            from = Logfiles.new(from_io)
            write_prologue(to_io, from.endian_swap ^ Pocolog.big_endian?)

            from.each_block_header(true) do |block_info|
                payload = from.block_stream.read_payload
                if block_info.type == STREAM_BLOCK
                    stream = BlockStream::StreamBlock.parse(payload)
                    write_stream_declaration(to_io, stream.index,
                            mappings[stream.name] || stream.name,
                            stream.type, nil, stream.metadata)
                else
                    Logfiles.write_block(to_io, block_info.kind, block_info.stream_index, payload)
                end
            end
        end
    end
end

