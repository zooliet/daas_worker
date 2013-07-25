#!/usr/bin/env ruby
# encoding: utf-8

# require 'bundler'
# Bundler.require

# require "rubygems"
require 'bunny'
require 'eventmachine'

module DAAS
	class Consumer
		attr_reader	:ipaddress

		def initialize(options)
			puts "Creating a consumer instance"
			@ipaddress = options[:ip]    
		end 

		def run
			user_name = "consumer"
			rcount = 1
			puts "*** Conneting to the Broker #{rcount}, #{user_name}"
			hostinfo = "amqp://guest:guest@#{ipaddress}:5672"

			puts "Connecting host info : #{hostinfo}"
			conn = Bunny.new(hostinfo)
			conn.start

			puts "*** Creating a channel in the connection."
			ch = conn.create_channel

			# to set round-robin distribution between workers
			ch.prefetch(1)

			puts "*** Making a queue"
			worker_queue  = ch.queue("daas.workers1", :auto_delete => true).bind("daas.producer")

			puts "*** Making default exchange to reply message "
			x  = ch.default_exchange

			EM.run do

              # gracefully exit & clean resources
              Signal.trap("INT") { 
                    EM.stop
                    conn.close
              }   
              Signal.trap("TERM") {
                    EM.stop
                    conn.close
              }

			  msg = ''

			  worker_queue.subscribe(:ack => true) do |delivery_info, metadata, payload|
				# puts "Reply queue: #{metadata.reply_to} #{deliver_info.exchange}"

				if metadata[:headers]["type"] == 'type_2'
					i = metadata[:headers]["chunk_index"]
					total_chunks = metadata[:headers]["total_chunks"]
					ofile = metadata[:headers]["out_file"]
					header = {type: 'type_2', chunk_index: i, total_chunks: total_chunks,out_file: ofile}

					puts "Reply queue: #{metadata.reply_to}:#{metadata.message_id}:#{i}:#{total_chunks}:#{ofile}:"
					msg = payload
					sleep(rand(rcount))
					ch.acknowledge(delivery_info.delivery_tag, false)
					x.publish(msg, :message_id => metadata.message_id, :routing_key => metadata.reply_to, headers: header )
					puts "Consumer reply"
				else
					ch.acknowledge(delivery_info.delivery_tag, false)
					puts "<< Received msg : #{payload}, #{Time.now}"
				end
			  end
			end

			puts "*** Closing connection"
			conn.close
		end

	end
end
