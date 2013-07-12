#!/usr/bin/env ruby
# encoding: utf-8

require 'bunny'
require 'eventmachine'

module DAAS
  class Consumer
    attr_reader :broker_ip
    
    def initialize(options)
      puts "Creating a consumer instance"
      @broker_ip = options[:ip]      
    end
    
    def run
      EM.run do
        puts "*** Conneting to the Broker"
        conn = Bunny.new("amqp://guest:guest@#{broker_ip}")
        conn.start

        puts "*** Creating a channel in the connection."
        ch = conn.create_channel
        ch.direct("daas.producer")

        puts "*** Making a queue"
        recv_q  = ch.queue("daas.producer.to.consumer", :auto_delete => true)
        x = ch.direct("daas.producer")
        recv_q.bind(x, routing_key: recv_q.name)
        
        # x  = ch.default_exchange

        recv_q.subscribe do |delivery_info, metadata, payload|
          puts "#{Time.now} : #{payload}"
          if payload =~ /quit/
            puts "*** Closing connection"
            conn.close
            EM.stop 
          end
        end
      end
    end
  end
end