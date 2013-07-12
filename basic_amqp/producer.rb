#!/usr/bin/env ruby
# encoding: utf-8

require 'bunny'
require 'eventmachine'

module DAAS
  
  class Producer
    attr_reader :broker_ip
    
    def initialize(options)
      puts "Creating a producer instance"
      @broker_ip = options[:ip]
    end
        
    def run
      EM.run do
        puts "*** Conneting to the Broker"
        conn = Bunny.new("amqp://guest:guest@#{broker_ip}")
        conn.start

        puts "*** Creating a channel in the connection."
        ch = conn.create_channel

        puts "*** Making a queue and direct exchange"
        send_queue  = ch.queue("daas.producer.to.consumer", :auto_delete => true)
        x = ch.direct("daas.producer")
        # send_queue.bind(x)
        # x  = ch.default_exchange

        EM.open_keyboard(KeyboardHandler, x, send_queue, conn)
      end
    end
  end
end


class KeyboardHandler < EM::Connection
  include EM::Protocols::LineText2
  attr_reader :x, :send_q, :conn  

  def initialize(x, send_q, conn)
    @x = x
    @send_q = send_q
    @conn = conn
  end
    
  def receive_line(data)
    puts ">>> #{data}"
    x.publish(data, routing_key: send_q.name)
    if data =~ /exit/
      puts "*** Closing connection"
      conn.close
      EM.stop
    end
  end
end



