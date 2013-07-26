#!/usr/bin/env ruby
# encoding: utf-8
# require "rubygems"
require 'bunny'
require 'eventmachine'
require 'streamio-ffmpeg'

module DAAS


	class Producer
		attr_reader :ipaddress, :ifilename, :ofilename, :default_split_duration, :profile_type, :to_media, :from_media
		attr_writer :default_split_duration, :profile_type, :to_media, :from_media, :ifilename, :ofilename
    

		def initialize(options)
			puts "Creating a producer instance"
			if options.length  < 4
				puts "Usage : procuder.rb ipaddress filename outfilename [ipaddress]"
#				return false
			end
			@ipaddress  = options[:ip]
			@ifilename  = options[:in]  if options[:in]
			@ofilename  = options[:out] if options[:out]
			@default_split_duration = options[:dur] if options[:dur]
			@profile_type = options[:pro] if options[:pro]

			puts ":#{ipaddress}:#{ifilename}:#{ofilename}:#{default_split_duration}:#{default_split_duration.class}:"
		end 
    

		def split_mp4(ifilename, ofilename)
			default_split_duration_fmt = " -t #{Time.at(default_split_duration.to_i).gmtime.strftime('%R:%S')} "
			#trail_options              = " -bsf:v h264_mp4toannexb -f mpegts  "
			header_info = []
			
			# extract source file media type
			@from_media = ifilename.split(".")[1]
			if from_media == nil
				puts "File type doesn't support "
				return false
			end

			# extract destinamtion file & media type
			 info_file = ofilename.split(".")[0]
			@to_media  = ofilename.split(".")[1]
			if to_media == nil
				puts "File type doesn't support "
				return false
			end

			default_option_string = "ffmpeg -i #{ifilename} -acodec copy -vcodec copy -ss "	
			if ifilename != nil and ofilename != nil
				@fp = 0
				begin
					@fp = FFMPEG::Movie.new(ifilename)
					rescue Exception => e
					puts "No such file #{ifilename} "
					return false
				end
				duration = @fp.duration
				i = 0
				offset = 0
				while offset < duration do
					start  = offset
					offset = offset + default_split_duration.to_i
					outfile = "#{info_file}i-#{i.to_s}.#{from_media}"
					option_string = default_option_string + Time.at(start).gmtime.strftime('%R:%S') + default_split_duration_fmt  + outfile
					puts option_string
					system(option_string)
					i = i +1
					header_info[i] = outfile
				end

				File.open("#{info_file}.header","w+") do | f |
					if f == nil
						puts "Header information file create error: #{info_file}.header"
						break
					end

					# make up header information for transmitting file
					# file header information field
					# input_file output_file profile input_file_extension output_file_extension number_of_split_file
					# ex) test.mp4 test.avi profile1 mp4 avi 7
					header_info[0] = "#{ifilename} #{ofilename} #{profile_type} #{from_media} #{to_media} #{i}"
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
			profile_type = ""
			to_media   = ""
			from_media =""
			sfile =""
			info_file = filename.split(".").first
			if filename
			  i = 0
			  f = File.open("#{info_file}.header") 
			  f.each_line do | fline|

				 # First line in the given file is information field.
				 # Second... Last lines are filename to be transmitted.

				 header_info = []
				 puts "line#{i}:#{fline}:"
				 if i == 0
				    header_info = fline.split(" ")	
					# file header information field
					# input_file output_file profile input_file_extension output_file_extension number_of_split_file
					puts " File header info : #{header_info[0]} #{header_info[1]}, #{header_info[2]}, #{header_info[3]},#{header_info[4]}, #{header_info[5]}"
					ofilename= header_info[1].split(".").first
					profile_type= header_info[2]
					from_media= header_info[3]
					to_media= header_info[4]
					chunks = header_info[5]
				 else
					sfile = "#{ofilename}o-#{i}.#{to_media}"	 	
					tf = File.open(fline.strip)
				    if tf != nil
					  puts "*** Publishing a chunk #{i}"
					  x.publish(tf.sysread(tf.size), reply_to: recv_queue.name, message_id: i, headers: {type: 'type_2', chunk_index: i, total_chunks: chunks, out_file: sfile, profile: profile_type, mtype: from_media })
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

		def save_file(filename,content)

			if File.file?(filename)
				puts "File name already exist. #{filenmae}"
				return false
			end

			f = File.open(filename,"w+")
			f.write(content)
			f.close

		end

		def merge_mp4(ofilename, file_list)
			#default_option_string = "ffmpeg -i "concat:tt-1.ts|tt-2.ts|tt-3.ts|tt-4.ts" -c copy -bsf:a aac_adtstoasc output.mp4"
			#default_string = "ffmpeg -i \"concat:#{file_list}\" -c copy -bsf:a aac_adtstoasc #{filename}.mp4"
			#default_string = "ffmpeg -f concat -i \"concat:#{file_list}\" -c copy #{ofilename}"

			# extract destinamtion file & media type
			puts "#{ofilename}:#{file_list}"
			info_file   = ofilename.split(".")[0]
			media_type  = ofilename.split(".")[1]

			puts " merge: #{info_file}:#{media_type}"
			# make up concatenate file
			concat_file = "#{info_file}o.cat"
			f = File.open(concat_file,"w+")
			file_list.split("|").each do |line|
				f.write("file .\/#{line}\n")
			end
			f.close

			# make up system call command 
			default_string = "ffmpeg -f concat -i #{concat_file} -c copy #{ofilename}"

			puts default_string
			system(default_string)

			puts "Remove temporary files"
			system("rm #{info_file}.header #{info_file}i* #{info_file}o*")
			#system("rm #{info_file}.header")
			
			timediff = Time.now - @timespan

			puts "Receive:: <<< #{Time.now}  Elapsed time : #{timediff}"
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
			producer_reply   = ch.queue("daas.reply", :auto_delete => true )
			producer_reply.bind(producer_transmit,:routing_key => producer_reply.name)

			EM.run do
			  
			  # gracefully exit & clean resources
			  Signal.trap("INT") { 
			  		EM.stop
					producer_transmit.delete
					conn.close
					puts "*** Closing connection by user interrupt INT"
					}
			  Signal.trap("TERM") {
			  		EM.stop
					producer_transmit.delete
					conn.close
					puts "*** Closing connection by user interrupt TERM"
					}

			  join_storage_in_hash = {}
			  total_chunks =0
			  producer_reply.subscribe do |delivery_info, metadata, payload|
				if metadata[:headers]["type"] == 'type_2'
					 i = metadata[:headers]["chunk_index"]
					 total_chunks = metadata[:headers]["total_chunks"]
					 return_file = metadata[:headers]["out_file"]
					 puts "*** File chunk #{i} has returned back #{total_chunks}:#{return_file}:"

					 save_file(return_file, payload )
					 join_storage_in_hash.merge!({i => return_file})

					 puts "total_chunk:#{total_chunks}:#{join_storage_in_hash.length}:"
					 # if total_chunks == join_storage_in_hash.length
					 if join_storage_in_hash.length == total_chunks.to_i
						 puts "*** Merging chunks....."
						 merge_file_list = join_storage_in_hash.sort.map {|k,v| v}.join("|")
						 merge_mp4( ofilename, merge_file_list)
						 total_chunks = 0
						 join_storage_in_hash = {}
					 end

				 else
					  puts "<<< #{Time.now} : #{payload}"
				 end
			   end

			   puts "*** Sending message"
			   if ifilename != nil and ofilename != nil
				   e = split_mp4(ifilename,ofilename)
				   puts "split:: <<< #{Time.now}"
				   if e != false
					   transmit(producer_transmit,producer_reply, ofilename)
					end
				end

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

		puts "Usage: cvt --in filename.mp4 --out filename.avi"
		puts "Usage: duration seconds"
		puts "Usage: profile  profile_type,  ex) profile1, profile2"
		puts "Usage: media    ex) supported media type  avi,mp4,flv,etc"
		puts "Usage: msg send_message_string"
		puts "Usage: help "
		puts "Usage: quit "

	end

	def command_parser(data)

		profile_list = ['profile1','profile2','profile3']
		media_list   = ['avi','mp4','flv']

		command = data.split(" ").first
		# command line parser
		case command
		when 'help'
			yield  :help , nil, nil
		when 'quit'
			yield  :quit , nil, nil
		when 'msg'
			yield  :msg , nil, nil
		when 'cvt'
			line = []
			line = data.split(" ")
			if line.length == 5
				if line[1] == "--in" and line[3] == "--out"
					yield  :cvt, line[2], line[4] 
					return true
				end
			end
			puts "Usage: cvt --in filename.mp4 --out filename"
			yield :cvt, nil, nil 
		when 'duration'
			line = []
			line = data.split(" ")
			if line.length == 2
				yield :duration, line[1], nil
				return true
			end
			puts "Usage: duration seconds"
			yield :duration, nil, nil 
		when 'profile'
			line = []
			line = data.split(" ")
			if line.length == 2
				if profile_list.include?( line[1] )
					yield :profile, line[1], nil
					return true
				end
			end
			puts "Usage: profile  profile_type,  ex) profile1, profile2"
			yield :profile, nil, nil 
		when 'media'
			line = []
			line = data.split(" ")
			if line.length == 2
				if media_list.include?( line[1] )
					yield :media, line[1], nil
					return true
				end
			end
			puts "Usage: media    media_type,    ex) avi,mp4,etc"
			yield :media, nil, nil 
		else
			yield nil, nil, nil
		end
	end
		
	def receive_line(data)
		puts ">>> #{data}"
		if data.size > 0

			command_parser(data) do | command, infile, outfile|
		  	  
			case command
			when :cvt
				puts " cmd parsing :#{command}:#{infile}:#{outfile}:"
				if infile != nil and outfile != nil
					puts "*** Sending message"
					@inst.ifilename = infile
					@inst.ofilename = outfile
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
					puts "Change split duration to #{@inst.default_split_duration}"
				end

			when :profile
				puts " cmd parsing :#{command}:#{infile}:#{outfile}:"
				if infile != nil 
					@inst.profile_type = infile
					puts "Change profile type  to #{@inst.profile_type}"
				end

			when :media
				puts " cmd parsing :#{command}:#{infile}:#{outfile}:"
				if infile != nil 
					@inst.to_media = infile
					puts "Change media type to #{@inst.to_media}"
				end
			when :help
				command_helper()
			when :quit
				@dexchanger.delete
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

