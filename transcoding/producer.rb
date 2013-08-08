#!/usr/bin/env ruby
# encoding: utf-8

module DAAS
  module Transcoding
    class Producer
      attr_reader :ipaddress  		
  		attr_accessor :in_filename, :out_filename, :to_media, :from_media, 
  		              :profile_type, :split_duration  		  		
      
      def initialize(options)
        puts "*** DAAS::Transcoding::Producer start"
  			@ipaddress      = options[:ip]
  			@in_filename    = options[:in]  if options[:in]
  			@out_filename   = options[:out] if options[:out]
  			@split_duration = options[:dur] || 60
  			@profile_type   = options[:pro] || 'profile1'
  			
        # puts "#{ipaddress}:#{in_filename}:#{out_filename}:#{split_duration}:#{split_duration.class}:"
      end
      
  		def split_asset(infile, outfile)
  		  @split_start_time = Time.now
  		  puts "*** Splitting starts at #{@split_start_time}"
  			  		  
        # extract source file media type
  			self.from_media = infile.split(".")[1].downcase
        unless ['avi', 'mp4', 'flv'].include?(from_media)
  				puts "[ERROR] File type doesn't support "
  				return false
  			end

  			# extract destinamtion file & media type
  			info_file, self.to_media = outfile.split(".")
        unless ['avi', 'mp4', 'flv'].include?(to_media)
  				puts "[ERROR] File type doesn't support "
  				return false
  			end

        split_duration_fmt = " -t #{Time.at(split_duration.to_i).gmtime.strftime('%R:%S')} "
  			#trail_options              = " -bsf:v h264_mp4toannexb -f mpegts  "
  			default_option_string = "ffmpeg -y -i #{infile} -acodec copy -vcodec copy -ss "	

        movie = FFMPEG::Movie.new(infile)
  			duration = movie.duration

  			header_info = []
  			split_info = {chunks: []}  			
  			i = 0
  			offset = 0

  			while offset < duration do
					i = i + 1
  			  start  = offset
  				offset = offset + split_duration.to_i
  				output = "#{info_file}i-#{i}.#{from_media}"
  				option_string = default_option_string + Time.at(start).gmtime.strftime('%R:%S') + split_duration_fmt + output
          # puts "*** Command executed: #{option_string}"
          system("#{option_string} &> /dev/null")
					header_info[i] = output
					split_info[:chunks] << output
  			end
  			
  			begin
  			  split_info[:total_chunks] = split_info[:chunks].length
  			  split_info[:infile]       = infile
  			  split_info[:out_file]     = info_file
  			  split_info[:profile]      = self.profile_type
  			  split_info[:mtype]        = self.from_media
  			end

				File.open("#{info_file}.header","w+") do | f |
					# make up header information for transmitting file
					# file header information field
					# input_file output_file profile input_file_extension output_file_extension number_of_split_file
					# ex) test.mp4 test.avi profile1 mp4 avi 7
					header_info[0] = "#{infile} #{outfile} #{profile_type} #{from_media} #{to_media} #{i}"
					f.write( header_info.join("\n") )
				end

        split_end_time = Time.now
  			splitting_time = split_end_time - @split_start_time
  			puts "*** Splitting ends at #{split_end_time}" 
  			puts "*** Splitting takes #{splitting_time} seconds"
  			
  			yield(split_info) if block_given?
      end
      
      def transmit (x, recv_queue, filename)
  			transmit_start_time = Time.now
  			puts "*** Transmit starts at #{transmit_start_time}"

        # chunks =0
        # ofilename=""
        # profile_type = ""
        # to_media   = ""
        # from_media =""
        # sfile =""
        # 
        # # filename: out_filename with extention
        # info_file = filename.split(".").first
        # 
        # if filename
        #   i = 0
        #   f = File.open("#{info_file}.header") 
        #   f.each_line do | fline|
        # 
        #    # First line in the given file is information field.
        #    # Second... Last lines are filename to be transmitted.
        # 
        #    header_info = []
        #    # puts "line#{i}:#{fline}:"
        #    if i == 0
        #       header_info = fline.split(" ")  
        #     # file header information field
        #     # input_file output_file profile input_file_extension output_file_extension number_of_split_file
        #     puts " File header info : #{header_info[0]} #{header_info[1]}, #{header_info[2]}, #{header_info[3]},#{header_info[4]}, #{header_info[5]}"
        #     ofilename= header_info[1].split(".").first
        #     profile_type= header_info[2]
        #     from_media= header_info[3]
        #     to_media= header_info[4]
        #     chunks = header_info[5]
        #    else
        #     sfile = "#{ofilename}o-#{i}.#{to_media}"    
        #     tf = File.open(fline.strip)
        #       if tf != nil
        #       puts "*** Publishing chunk #{i}:#{fline.strip}"
        #       x.publish(tf.sysread(tf.size), reply_to: recv_queue.name, message_id: i, headers: {type: 'type_2', chunk_index: i, total_chunks: chunks, out_file: sfile, profile: profile_type, mtype: from_media })
        #     end    
        #     tf.close
        #    end    
        #    i =i+1
        #   end
        #    f.close
        # end

  			transmit_end_time = Time.now
  			transmitting_time = transmit_end_time - transmit_start_time
  			puts "*** Transmit ends at #{transmit_end_time}." 
  			puts "*** Transmitting takes #{transmitting_time} seconds."


  		end
      
      def save_file(filename, content)
        File.open(filename, 'w') do |f|
          f.write(content)
        end
          
        # unless File.file?(filename)
        #           File.open(filename,"w+") do |f|
        #             f.write(content)
        #           end         
        # else
        #   puts "[ERROR] File name already exist. #{filenmae}"
        #   return false
        # end        
  		end
  		
  		def merge_asset(filename, file_list)
  			merge_start_time = Time.now
  			puts "*** Merging starts at #{merge_start_time}"

        # filename is out_filename with extention
  			info_file, media_type = filename.split(".")  
  			# puts " merge: #{info_file}:#{media_type}"
  			
  			# make up concatenate file
        concat_file = "#{info_file}o.cat"
        # or we can say: concat_file = "#{Time.now.strftime("%H%M%S")}.cat"
        
  			File.open(concat_file,"w+") do |f|
  			  file_list.each do |file|
  			    f.write("file .\/#{file}\n")
  			  end
  			end

  			# make up system call command 
  			default_string = "ffmpeg -y -f concat -i #{concat_file} -c copy #{filename}"
  			# puts default_string
        # system("#{default_string}")
        system("#{default_string} &> /dev/null")

  			puts "*** Remove temporary files"
        FileUtils.rm Dir.glob("#{info_file}i*")    
        FileUtils.rm Dir.glob("#{info_file}o*")    
  			FileUtils.rm Dir.glob("#{info_file}.header") 
  			# system("rm #{info_file}.header #{info_file}i* #{info_file}o* &> /dev/null")
  			
  			merge_end_time  = Time.now
  			merging_time    = merge_end_time - merge_start_time
  			splitting_to_merging_time = merge_end_time - @split_start_time

  			puts "*** Merging ends at #{merge_end_time}"
  			puts "*** Merging takes #{merging_time} seocnds"
  			puts "*** Total Elapsed time : #{splitting_to_merging_time}"
  		end  		
  		
      def run
  			hostinfo = "amqp://guest:guest@#{ipaddress}:5672"
  			puts "*** Connet to the Broker: #{hostinfo}"
  			conn = Bunny.new(hostinfo)
  			conn.start

  			puts "*** Create a channel in the connection"
  			ch = conn.create_channel
  			
  			puts "*** Make a direct exchage for transmit"
  			producer_transmit = ch.direct("daas.producer")
  			
  			puts "*** Make a reply queue"
  			producer_reply   = ch.queue("daas.reply", auto_delete: true )
        # producer_reply.bind(producer_transmit,:routing_key => producer_reply.name)

        EM.run do
          # gracefully exit & clean resources
          Signal.trap("INT") do
            EM.stop
            conn.close
            puts "*** Close connection by user interrupt INT"
          end
          Signal.trap("TERM") do
            EM.stop
            conn.close
            puts "*** Closing connection by user interrupt TERM"
          end
          
  			  join_storage_in_hash = {}
  			  total_chunks =0
  			  producer_reply.subscribe do |delivery_info, metadata, payload|
            # puts "***#{metadata}"
  			    if metadata[:headers]["type"] == 'type_2'
  			      i             = metadata[:headers]["chunk_index"]
  			      total_chunks  = metadata[:headers]["total_chunks"]
  			      return_file   = metadata[:headers]["out_file"]
  			      puts "*** File chunk #{i}/#{total_chunks} has returned back : #{return_file}"
  			      
  			      save_file(return_file, payload)
  			      join_storage_in_hash.merge!({i => return_file})
  			      
  			      if join_storage_in_hash.length == total_chunks.to_i
                # merge_file_list = join_storage_in_hash.sort.map {|k,v| v}.join("|")
                merge_file_list = join_storage_in_hash.sort.map {|k,v| v}
  			        merge_asset(out_filename, merge_file_list)

                # total_chunks = 0
                # join_storage_in_hash = {}
  			        
  			        EM.stop()
  			      end
  			    else
  			      puts "<<< #{Time.now} : #{payload}"
  			    end
          end  # End of subscribe
          
          if in_filename != nil and out_filename != nil
            e = split_asset(in_filename, out_filename) do |split_info|
              split_info[:chunks].each_with_index do |chunkfile, i|
                File.open(chunkfile) do |f|
                  puts "*** Publishing a chunk #{i+1} : #{chunkfile}"
      					  producer_transmit.publish(
      					                  f.read(f.size), 
      					                  routing_key: 'daas.workers',  # by hl1sqi
      					                  reply_to: producer_reply.name, message_id: i+1, 
      					                  headers: { type: 'type_2', 
      					                            chunk_index: i+1, 
      					                            total_chunks: split_info[:chunks].length, 
      					                            out_file: "#{split_info[:out_file]}o-#{i+1}.#{to_media}",
      					                            profile: split_info[:profile], 
      					                            mtype: split_info[:mtype] }      					  
      					  )
      					end                					
              end
            end
            # e = split_asset(in_filename, out_filename)
            # if e != false
            #   transmit(producer_transmit, producer_reply, out_filename)
            # end
          end
          
        end # End of EM.run
                
        puts "*** Close connection"
  			conn.close  			
      end
    end
  end
end