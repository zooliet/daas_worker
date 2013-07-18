#!/usr/bin/env ruby
# encoding: utf-8
# require "rubygems"
require 'bunny'
require 'eventmachine'
require 'streamio-ffmpeg'

module DAAS


  
	class Producer
		attr_reader :ipaddress, :ifilename, :ofilename, :default_split_duration
    
		def initialize(options)
			puts "Creating a producer instance"
			if options.length  < 4
				puts "Usage : procuder.rb ipaddress filename outfilename [ipaddress]"
				return false
			end
			@ipaddress = options[:ip]
			@ifilename = options[:in]  if options[:in]
			@ofilename = options[:out] if options[:out]
			@default_split_duration = options[:dur] if options[:dur]

			puts ":#{ipaddress}:#{ifilename}:#{ofilename}:#{default_split_duration}:#{default_split_duration.class}:"
		end 
    

		def split_mp4(ifilename, ofilename)
			default_split_duration_fmt = " -t #{Time.at(default_split_duration.to_i).gmtime.strftime('%R:%S')}"
			trail_options              = " -bsf:v h264_mp4toannexb -f mpegts  "
			header_info = []
			
			#default_option_string = "ffmpeg -acodec copy -vcodec copy -ss 0 -t 00:15:00 -i ORIGINALFILE.mp4 OUTFILE-1.mp4"	
			default_option_string = "ffmpeg -i #{ifilename} -acodec copy -vcodec copy -ss "	
			if ifilename != nil and ofilename != nil
				fp = FFMPEG::Movie.new(ifilename)
				duration = fp.duration
				i = 0
				offset = 0
				while offset < duration do
					start  = offset
					offset = offset + default_split_duration.to_i
					outfile = "#{ofilename}i-#{i.to_s}.ts"
					option_string = default_option_string + Time.at(start).gmtime.strftime('%R:%S') + default_split_duration_fmt + trail_options + outfile
					puts option_string
					system(option_string)
					i = i +1
					header_info[i] = outfile
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
			puts "this is split method"
		end

		def transmit ( x,recv_queue,filename )

			chunks =0
			ofilename=""
			if filename
			  i = 0
			  f = File.open("#{filename}.header") 
			  f.each_line do |line|

				 # First line in the given file is information field.
				 # Second... Last lines are filename to be transmitted.

				 puts "line#{i}:#{line}:"
				 if i == 0
				    header_info = line.split(" ")	
					puts " File prefix name : #{header_info[0]}, number of files : #{header_info[2]}"
					ofilename= header_info[0]
					chunks = header_info[2]
				 else
					tf = File.open(line.strip)
				    if tf != nil
					  puts "*** Publishing a chunk #{i}"
					  x.publish(tf.sysread(tf.size), reply_to: recv_queue.name, message_id: i, headers: {type: 'type_2', chunk_index: i, total_chunks: chunks, out_file: ofilename })
					end    
					tf.close
				 end    
				 i =i+1
			  end
			end


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
			puts default_string
			system(default_string)

			puts "Remove temporary files"
			system("rm #{filename}.header")
			system("rm #{filename}*.ts")
			
		end

		def run
			puts "*** Conneting to the Broker"
			hostinfo = "amqp://guest:guest@#{ipaddress}:5672"

			puts "Connecting host info : #{hostinfo}"
			conn = Bunny.new(hostinfo)
			conn.start

			puts "*** Creating a channel in the connection."
			ch = conn.create_channel

			puts "*** Making worker & reply queue"
			producer_transmit  = ch.direct("daas.producer")
			#producer_worker  = ch.queue("daas.workers", :auto_delete => true)
			producer_reply   = ch.queue("daas.reply", :auto_delete => true )
			producer_reply.bind(producer_transmit,:routing_key => producer_reply.name)

			EM.run do

			  join_storage_in_hash = {}
			  total_chunks =0
			  producer_reply.subscribe do |delivery_info, metadata, payload|
				if metadata[:headers]["type"] == 'type_2'
					 i = metadata[:headers]["chunk_index"]
					 total_chunks = metadata[:headers]["total_chunks"]
					 ofilename = metadata[:headers]["out_file"]
					 puts "*** File chunk #{i} has returned back #{total_chunks}:#{ofilename}:"

					 save_file(ofilename, i, payload )
					 join_storage_in_hash.merge!({i => "#{ofilename}o-#{i}.ts"})

					 puts "total_chunk:#{total_chunks}:#{join_storage_in_hash.length}:"
					 # if total_chunks == join_storage_in_hash.length
					 if join_storage_in_hash.length == total_chunks.to_i
						 puts "*** Merging chunks....."
						 merge_file_list = join_storage_in_hash.sort.map {|k,v| v}.join("|")
						 merge_mp4(ofilename, merge_file_list)
					 end

				 else
					  puts "<<< #{Time.now} : #{payload}"
				 end
			   end

			   puts "*** Sending message"
			   split_mp4(ifilename,ofilename)
			   transmit(producer_transmit,producer_reply, ofilename)

			   # producer_transmit.publish(fmessage, :reply_to => producer_reply.name)
			   EM.open_keyboard(KeyboardHandler, producer_transmit, producer_reply)
			end

			puts "*** Closing connection"
			conn.close
		end

	end

end


class KeyboardHandler < EM::Connection
	include EM::Protocols::LineText2

	def initialize(dexchanger, reply_queue)
		@dexchanger = dexchanger
		@reply_queue = reply_queue
	end
		
	def receive_line(data)
		puts ">>> #{data}"
		if data.size > 0
		  @dexchanger.publish(data, :reply_to => @reply_queue.name)
		  if data =~ /quit/
			EM.stop
		  end
		end
	end
end

