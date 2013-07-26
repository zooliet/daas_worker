#!/usr/bin/env ruby
# encoding: utf-8
# require "rubygems"

require 'bunny'
require 'eventmachine'
require 'streamio-ffmpeg'

module DAAS
	class Producer
		attr_reader :ipaddress, :ifilename, :ofilename, :default_split_duration
		attr_writer :default_split_duration
    
		def initialize(options)
			puts "*** Creating a producer instance"
			if options.length  < 4
				puts "Usage : procuder.rb ipaddress filename outfilename [ipaddress]"
        # return false
			end
			@ipaddress = options[:ip]
			@ifilename = options[:in]  if options[:in]
			@ofilename = options[:out] if options[:out]
			@default_split_duration = options[:dur] if options[:dur]

      # puts ":#{ipaddress}:#{ifilename}:#{ofilename}:#{default_split_duration}:#{default_split_duration.class}:"
		end 
    
		def split_mp4(ifilename, ofilename, &block)
		  FileUtils.rm Dir.glob("#{ofilename}**") unless Dir.glob("#{ofilename}**").empty?
		  
			default_split_duration_fmt = " -t #{Time.at(default_split_duration.to_i).gmtime.strftime('%R:%S')}"
			trail_options              = " -bsf:v h264_mp4toannexb -f mpegts  "
			default_option_string = "ffmpeg -i #{ifilename} -acodec copy -vcodec copy -ss "	
			
			header_info = []  

      # if ifilename != nil and ofilename != nil
      #   # @fp = 0
      #   begin
      #     @fp = FFMPEG::Movie.new(ifilename)
      #   rescue Exception => e
      #     puts "No such file #{ifilename} "
      #     return false
      #   end
      
      unless ifilename.empty? or ofilename.empty?
        movie = FFMPEG::Movie.new(ifilename)
				duration = movie.duration
				
				chunks = (duration / default_split_duration.to_f).ceil
        # puts "*** We will have #{chunks} chunks"

				i = 0
				offset = 0
				while offset < duration do
					start  = offset
					offset = offset + default_split_duration.to_i
					outfile = "#{ofilename}i-#{i}.ts"
					
					option_string = default_option_string + Time.at(start).gmtime.strftime('%R:%S') + default_split_duration_fmt + trail_options + outfile					
          # puts option_string
          puts "*** Command executed: #{option_string}"    			
					system("#{option_string} &> /dev/null")

					i = i + 1					
					header_info[i] = outfile					
					
					yield(outfile, i, chunks) if block_given?
					
				end

				File.open("#{ofilename}.header","w+") do | f |
					if f == nil
						puts "Header information file create error: #{ifilename}.header"
						break
					end

					# make up header information for transmitting file
					# file prefix, number of files, etc
					# ex) ofilename 7
					header_info[0] = "#{ofilename} number_of_files #{i}"
					f.write( header_info.join("\n") )
					f.close
				end

			else
				puts "Needs input and output filenames"
			end
		end

		def transmit (x, recv_queue,filename )

			chunks =0
			ofilename=""
			if filename
			  i = 0
			  f = File.open("#{filename}.header") 
			  f.each_line do | fline|

				 # First line in the given file is information field.
				 # Second... Last lines are filename to be transmitted.

				 header_info = []
				 puts "line#{i}:#{fline}:"
				 if i == 0
				    header_info = fline.split(" ")	
					puts " File prefix name : #{header_info[0]}, number of files : #{header_info[2]}"
					ofilename= header_info[0]
					chunks = header_info[2]
				 else
					tf = File.open(fline.strip)
				    if tf != nil
					  puts "*** Publishing a chunk #{i}"
					  x.publish(tf.sysread(tf.size), reply_to: recv_queue.name, message_id: i, headers: {type: 'type_2', chunk_index: i, total_chunks: chunks, out_file: ofilename })
					end    
					tf.close
				 end    
				 i =i+1
			  end
			   f.close
			end

			@timespan=Time.now
			puts "transmit:: <<< #{Time.now}"


		end

		def save_file(filename,i,content)

			if File.file?("#{filename}o-#{i}.ts")
				puts "File name already exist. #{filenmae}"
				return false
			end

			f = File.open("#{filename}o-#{i}.ts","w+")
			f.write(content)
			f.close

		end

		def merge_mp4(filename, file_list)
			#default_option_string = "ffmpeg -i "concat:tt-1.ts|tt-2.ts|tt-3.ts|tt-4.ts" -c copy -bsf:a aac_adtstoasc output.mp4"
			
			default_string = "ffmpeg -i \"concat:#{file_list}\" -c copy -bsf:a aac_adtstoasc #{filename}.mp4"
			puts "*** Command executed: #{default_string}"
			system("#{default_string} &> /dev/null")
			
			puts "*** Removing temporary files"
			FileUtils.rm Dir.glob("#{filename}**.ts")		  
			FileUtils.rm Dir.glob("#{filename}**.header")		  

      # 
      # system("rm #{filename}.header #{filename}*.ts")
      # 
      # timediff = Time.now - @timespan
      # 
      # puts "Receive:: <<< #{Time.now}  Elapsed time : #{timediff}"
		end

		def run
			puts "*** Conneting to the Broker"
			hostinfo = "amqp://guest:guest@#{ipaddress}:5672"

			puts "*** Connecting host info : #{hostinfo}"
			conn = Bunny.new(hostinfo)
			conn.start
			
			puts "*** Creating a channel in the connection."
			ch = conn.create_channel

			puts "*** Making worker & reply queue"
			producer_transmit  = ch.direct("daas.producer")			
			producer_reply = ch.queue("daas.reply", auto_delete: true )
			producer_reply.bind(producer_transmit, routing_key: producer_reply.name)
			
			EM.run do		  
			  # gracefully exit & clean resources
			  Signal.trap("INT") { 
			  		EM.stop
            # producer_transmit.delete by hl1sqi
					  conn.close
					  puts "*** Closing connection by user interrupt INT"
				}

			  Signal.trap("TERM") {
			  		EM.stop
            # producer_transmit.delete by hl1sqi
					  conn.close
					  puts "*** Closing connection by user interrupt TERM"
				}

			  join_storage_in_hash = {}
			  total_chunks =0
			  producer_reply.subscribe do |delivery_info, metadata, payload|
  				if metadata[:headers]["type"] == 'type_2'
            i = metadata[:headers]["chunk_index"]
            total_chunks = metadata[:headers]["total_chunks"]
            ofilename = metadata[:headers]["out_file"]
            puts "*** Chunk #{i}/#{total_chunks} has returned back."

            save_file(ofilename, i, payload )
            join_storage_in_hash.merge!({i => "#{ofilename}o-#{i}.ts"})

            # puts "total_chunk:#{total_chunks}:#{join_storage_in_hash.length}:"
            # if total_chunks == join_storage_in_hash.length
            if join_storage_in_hash.length == total_chunks.to_i
              puts "*** Start merging"
              merge_start_time = Time.now
              merge_file_list = join_storage_in_hash.sort.map {|k,v| v}.join("|")
              merge_mp4(ofilename, merge_file_list)
              total_chunks = 0
              join_storage_in_hash = {}
              puts "*** Merging takes #{Time.now - merge_start_time} seconds."
            end
  				else
            puts "<<< #{Time.now} : #{payload}"
  				end
			  end

        puts "*** Sending message"

        # by hl1sqi
        # e = split_mp4(ifilename, ofilename)
        # puts "split:: <<< #{Time.now}"
        # if e != false
        #   transmit(producer_transmit, producer_reply, ofilename)
        # end

        # by hl1sqi
        # begin
        #   puts "Split mp4:: Start at #{Time.now}"        
        #   split_mp4(ifilename, ofilename)          
        # rescue Exception => e
        #   puts "Split mp4:: #{e.message}: #{e.backtrace}"
        # else
        #   transmit(producer_transmit, producer_reply, ofilename)
        # ensure
        #   puts "Split mp4:: End at #{Time.now}"
        # end
        
        split_start_time = Time.now
        # puts "*** split_mp4:: Start at #{Time.now}"        
        
        split_mp4(ifilename, ofilename) do |tempfile, i, chunks|                  
          File.open(tempfile) do |f|
					  puts "*** Publishing a chunk #{i} (#{tempfile})"
					  producer_transmit.publish(
					                  f.read(f.size), 
					                  routing_key: 'daas.workers',  # by hl1sqi
					                  reply_to: producer_reply.name, message_id: i, 
					                  headers: {type: 'type_2', chunk_index: i, total_chunks: chunks, out_file: ofilename }
					  )
					end          
        end
        
        puts "*** Splitting takes #{Time.now - split_start_time} seconds."
        
        
			   # producer_transmit.publish(fmessage, :reply_to => producer_reply.name)
			   EM.open_keyboard(KeyboardHandler, producer_transmit, producer_reply, self)
			end

		#	puts "*** Closing connection"
		#	producer_transmit.delete
		#	conn.close
		end

	end

end

class KeyboardHandler < EM::Connection
	include EM::Protocols::LineText2

	def initialize(dexchanger, reply_queue, inst)
		@dexchanger = dexchanger
		@reply_queue = reply_queue
		@inst     = inst
	end

	def command_helper

		puts "Usage: mp4 --in filename.mp4 --out filename"
		puts "Usage: duration seconds"
		puts "Usage: msg send_message_string"
		puts "Usage: help "
		puts "Usage: quit "

	end

	def command_parser(data)

		# command line parser
		case
		when data.match(/help/)
			yield  :help , nil, nil
		when data.match(/quit/)
			yield  :quit , nil, nil
		when data.match(/msg/)
			yield  :msg , nil, nil
		when data.match(/mp4/)
			line = []
			line = data.split(" ")
			if line.length == 5
				if line[1] == "--in" and line[3] == "--out"
					yield  :mp4, line[2], line[4] 
					return true
				end
			end
			puts "Usage: mp4 --in filename.mp4 --out filename"
			yield :mp4, nil, nil 
		when data.match(/duration/)
			line = []
			line = data.split(" ")
			if line.length == 2
				yield :duration, line[1], nil
				return true
			end
			puts "Usage: duration seconds"
			yield :duration, nil, nil 
		else
			yield nil, nil, nil
		end
	end
		
	def receive_line(data)
		puts ">>> #{data}"
		if data.size > 0

			command_parser(data) do | command, infile, outfile|		  	  
  			case command
  			when :mp4
  				puts " cmd parsing :#{command}:#{infile}:#{outfile}:"
  				if infile != nil and outfile != nil
  					puts "*** Sending message"
  					e = @inst.split_mp4(infile, outfile)

  					puts "split:: <<< #{Time.now} #{e}"
  					if e != false
  						@inst.transmit(@dexchanger, @reply_queue, outfile)
  					end
  				end
  			when :duration
  				puts " cmd parsing :#{command}:#{infile}:#{outfile}:"
  				if infile != nil 
  					@inst.default_split_duration = infile.to_i
  				end
  				puts "Change split duration to #{@inst.default_split_duration}"

  			when :help
  				command_helper()
  			when :quit
          # @dexchanger.delete
  				EM.stop
  			when :msg
  				#puts " cmd parsing :#{data}:#{@dexchanger.name}:#{@reply_queue.name}:"
  				@dexchanger.publish(data.sub("msg",""), reply_to: @reply_queue.name, headers: {type: 'type_1'})
  		    else
  				puts " Unknown Command : type 'help' if you want to know commands "
  			end
		  end
		end
	end
end

