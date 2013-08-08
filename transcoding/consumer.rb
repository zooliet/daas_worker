#!/usr/bin/env ruby
# encoding: utf-8

module DAAS
  module Transcoding
    class Consumer
      attr_reader	:ipaddress
      
  		def initialize(options)
        puts "*** DAAS::Transcoding::Consumer start"
  			@ipaddress = options[:ip]    
      end
      
      def run
  			user_name = "consumer"
  			rcount    = 1
  			hostinfo  = "amqp://guest:guest@#{ipaddress}:5672"

  			puts "*** Connet to the Broker: #{hostinfo} (#{user_name}, #{rcount})"
  			conn = Bunny.new(hostinfo)
  			conn.start        

  			puts "*** Create a channel in the connection"
  			ch = conn.create_channel

  			# to set round-robin distribution between workers
  			ch.prefetch(1)
  			
  			puts "*** Make a direct exchage to bind with receive queue"
  			exchange = ch.direct("daas.producer")
  			
  			puts "*** Make a worker queue and bind it with the exchange above"
  			worker_queue  = ch.queue("daas.workers", auto_delete: true).bind(exchange, routing_key: "daas.workers")
  			
  			puts "*** Make a default exchange to reply message "
  			x  = ch.default_exchange
  			
  			EM.run do
          # gracefully exit & clean resources
          Signal.trap("INT") do
            EM.stop
            conn.close
          end
        
          Signal.trap("TERM") do
            EM.stop
            conn.close
          end

          worker_queue.subscribe(ack: true) do |delivery_info, metadata, payload|
            if metadata[:headers]["type"] == 'type_2'            
              i             = metadata[:headers]["chunk_index"]
              total_chunks  = metadata[:headers]["total_chunks"]
              out_file      = metadata[:headers]["out_file"]  # target's filename.format
              profile_type  = metadata[:headers]["profile"]
              media_type    = metadata[:headers]["mtype"]   # source's format
              
              header        = {type: 'type_2', chunk_index: i, total_chunks: total_chunks, out_file: out_file, profile: profile_type, mtype: media_type}
              # puts "*** Reply queue: #{metadata.reply_to}:#{metadata.message_id}:#{i}:#{total_chunks}:#{out_file}:#{profile_type}:#{media_type}:"

              # sleep(rand(rcount))
              reply_start_time = Time.now
              
              transcoding(payload, profile_type, out_file, media_type)
              File.open("#{out_file}c") do |f|
                unless f.nil?
                  x.publish(f.sysread(f.size), message_id: metadata.message_id, routing_key: metadata.reply_to, headers: header )
                end
              end

              replay_end_time = Time.now
              replying_time = replay_end_time - reply_start_time

              FileUtils.rm Dir.glob("#{out_file}c")     
              # system("rm #{ofile}c &> /dev/null")                                      
              
              ch.acknowledge(delivery_info.delivery_tag, false)              
            else
              ch.acknowledge(delivery_info.delivery_tag, false)
              puts "<< Received msg : #{payload}, #{Time.now}"
            end
          end
                      

        end
  			
  			puts "*** Close connection"
  			conn.close

      end

      def transcoding(content, profile, out_file, media)
        in_file = "#{out_file}.#{media}"
        f = File.open(in_file,"w+")
        f.write(content)
        f.close        

        # File.opne(ifile, 'w+') do |f|
        #   f.write(content)
        # end
        
        type = out_file.split(".")[1]  # actually it should be format, container or extention
        case type
  			when 'mp4'
  				cmd_string = "ffmpeg -y -i #{in_file} -acodec libfaac -vcodec h264 -f mp4 #{out_file}c"
  			when 'avi'
  				cmd_string = "ffmpeg -y -i #{in_file} -acodec mp3 -vcodec mpeg4 -vtag DIVX -f avi #{out_file}c"
  			when 'flv'
  				cmd_string = "ffmpeg -y -i #{in_file} -ar 44100 -acodec mp3 -vcodec flv -f flv #{out_file}c"
  			else
  				puts "Unkwon target transcoding type"
  				return false
  			end
  			
  			puts "*** Command executed: #{cmd_string}"    			        
        system( "#{cmd_string} &> /dev/null" )
        # system( "#{cmd_string}" )

  			FileUtils.rm Dir.glob( "#{in_file}" )
  			#system( "rm #{in_file}" )
  			
      end
    end
  end
end





			
			



