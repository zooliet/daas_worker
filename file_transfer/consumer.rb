#!/usr/bin/env ruby
# encoding: utf-8

require 'bunny'
require 'eventmachine'

module DAAS
  class Consumer
    attr_reader :broker_ip, :sleep_count
    
    def initialize(options)
      puts "Creating a consumer instance"
      @broker_ip = options[:ip]      
      @sleep_count = options[:sleep]
    end
    
    def run
      EM.run do
        puts "*** Conneting to the Broker"
        conn = Bunny.new("amqp://guest:guest@#{broker_ip}")
        conn.start

        puts "*** Creating a channel in the connection."
        ch = conn.create_channel
        ch.prefetch(1)

        puts "*** Making a queue"
        recv_q  = ch.queue("daas.producer.to.consumer", :auto_delete => true)
        x = ch.direct("daas.exchange")
        recv_q.bind(x, routing_key: recv_q.name)
        
        # x  = ch.default_exchange
        
        recv_q.subscribe(ack: true) do |delivery_info, metadata, payload|
          puts metadata.inspect
          if metadata[:headers]["type"] == 'type_2'
            i = metadata[:headers]["chunk_index"]
            total_chunks = metadata[:headers]["total_chunks"]
            header = {type: 'type_2', chunk_index: i, total_chunks: total_chunks}            

            puts "*** File chunk #{i} has arrived"
          else
            puts "<<< #{Time.now} : #{payload}"
            if payload =~ /quit/
              puts "*** Closing connection"
              conn.close
              EM.stop 
            end                              
            puts ">>> #{payload}"
            header = {}
          end

          sleep(sleep_count) # long-running task simulation
          puts "*** #{sleep_count} seconds count expired"
          ch.acknowledge(delivery_info.delivery_tag, false)
          x.publish(payload, routing_key: metadata.reply_to, headers: header)                    
        end
      end
    end
  end
end