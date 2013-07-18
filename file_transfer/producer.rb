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
      @infile = options[:in] if options[:in]
      @outfile = options[:out] if options[:out]
    end
        
    def run
      EM.run do
        puts "*** Conneting to the Broker"
        conn = Bunny.new("amqp://guest:guest@#{@broker_ip}:5672")
        conn.start

        puts "*** Creating a channel in the connection."
        ch = conn.create_channel

        puts "*** Making send & recv queues, direct exchange and binding them"
        send_queue  = ch.queue("daas.producer.to.consumer", auto_delete: true)
        recv_queue  = ch.queue("daas.consumer.to.producer", auto_delete: true)
        x = ch.direct("daas.exchange")
        recv_queue.bind(x, routing_key: recv_queue.name)
        
        if @infile
          self.split_file(@infile) do |chunks|
            for i in (0...chunks.length)
              puts "*** Publishing a chunk #{i}"
              x.publish(chunks[i], routing_key: send_queue.name, reply_to: recv_queue.name, 
                    message_id: i, headers: {type: 'type_2', chunk_index: i, total_chunks: chunks.length})
            end            
          end            
          join_storage_in_hash = {}        
        end
        
        EM.open_keyboard(KeyboardHandler, x, conn, send_queue, recv_queue)

        recv_queue.subscribe do |delivery_info, metadata, payload|
          if metadata[:headers]["type"] == 'type_2'
            i = metadata[:headers]["chunk_index"]
            total_chunks = metadata[:headers]["total_chunks"]
            puts "*** File chunk #{i} has returned back"

            join_storage_in_hash.merge!({i => payload})
            # puts join_storage_in_hash
            if total_chunks == join_storage_in_hash.length
              File.open(@outfile, 'w') do |f|
                puts "*** Merging chunks..."
                # content = join_storage_in_hash.values.join()     
                content = join_storage_in_hash.sort.map {|k,v| v}.join()
                puts "*** Writing to #{@outfile}"
                f.write(content)
              end
            end            
          else
            puts "<<< #{Time.now} : #{payload}"
          end
        end
      end
    end
        
    def split_file(name, chunk_size = 1000000)
      chunks = []
      i = 0
      
      File.open(name, 'r') do |f|
        size = f.size        

        while size > chunk_size do
          chunks[i] = f.read(chunk_size) 
          size -= chunk_size
          i += 1
        end
        
        chunks[i] = f.read(chunk_size)         
      end
      
      puts "*** We have #{chunks.length} chunks"            
      yield(chunks)
      
      # chunks      
    end    
  end
end


class KeyboardHandler < EM::Connection
  include EM::Protocols::LineText2
  attr_reader :x, :conn, :send_queue, :recv_queue

  def initialize(x, conn, send_queue, recv_queue)
    @x = x
    @conn = conn
    @send_queue = send_queue
    @recv_queue = recv_queue
  end
    
  def receive_line(data)
    puts ">>> #{data}"
    x.publish(data, routing_key: send_queue.name, reply_to: recv_queue.name, 
      headers: {type: 'type_1'})
    if data =~ /exit/
      puts "*** Closing connection"
      conn.close
      EM.stop
    end
  end
end



