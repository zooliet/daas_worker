$LOAD_PATH << File.expand_path(File.dirname(__FILE__))
require 'daas'

class Daas < Thor  
  include Thor::Actions

  desc "producer1 --ip <ip>", "Basic AMQP producer"
  method_option :ip, type: :string, default: '127.0.0.1'
  def producer1
    require File.expand_path('../basic_amqp/producer', __FILE__)
    producer = DAAS::Producer.new(options)
    producer.run
    
    # inside "basic_amqp" do
    #   require './producer'
    #   producer = DAAS::Producer.new(options)
    #   producer.run
    # end
	end

  desc "consumer1 --ip <ip>", "Basic AMQP consumer"
  method_option :ip, type: :string, default: '127.0.0.1'
  def consumer1
    require File.expand_path('../basic_amqp/consumer', __FILE__)
    consumer = DAAS::Consumer.new(options)
    consumer.run    
	end

  desc "producer2 --ip <ip> --in <filename> --out <filename>", "File sending producer"
  method_option :ip, type: :string, default: '127.0.0.1'
  method_option :in, type: :string
  method_option :out, type: :string, default: 'noname'
  def producer2
    require File.expand_path('../file_transfer/producer', __FILE__)
    producer = DAAS::Producer.new(options)
    producer.run
    
    # new_options = options.dup
    # options = new_options
    # options.merge!(in: File.join(File.expand_path('..', __FILE__), options[:in])) if options[:in]
    # options.merge!(out: File.join(File.expand_path('..', __FILE__), options[:out]))
    # 
    # inside "file_transfer" do
    #   require './producer'
    #   producer = DAAS::Producer.new(options)
    #   producer.run
    # end
	end

  desc "consumer2 --ip <ip> --sleep <sec>", "File receiving consumer"
  method_option :ip, type: :string, default: '127.0.0.1'
  method_option :sleep, type: :numeric, default: 3
  def consumer2    
    require File.expand_path('../file_transfer/consumer', __FILE__)
    consumer = DAAS::Consumer.new(options)
    consumer.run
	end

  desc "producer3 --ip <ip> --in <filename> --out <filename> --dur seconds --pro <profile>", "Video File sending producer"
  method_option :ip, type: :string, default: '127.0.0.1'
  method_option :in, type: :string
  method_option :out, type: :string
  method_option :dur, type: :numeric, default: 60
  method_option :pro, type: :string, default: 'profile1'
  def producer3
    require File.expand_path('../video_transfer/producer', __FILE__)
    producer = DAAS::Producer.new(options)
    producer.run
   end

  desc "consumer3 --ip <ip>", "Video File receiving consumer "
  method_option :ip, type: :string, default: '127.0.0.1'
  def consumer3
    require File.expand_path('../video_transfer/consumer', __FILE__)
    consumer = DAAS::Consumer.new(options)
    consumer.run    
 	end

  desc "producer4 --ip <ip> --in <filename> --out <filename> --dur seconds", "Video File sending producer"
  method_option :ip, type: :string, default: '127.0.0.1'
  method_option :in, type: :string
  method_option :out, type: :string
  method_option :dur, type: :numeric, default: 60
  def producer4
    require File.expand_path('../video_transfer_revisited/producer', __FILE__)
    producer = DAAS::Producer.new(options)
    producer.run
   end

  desc "consumer4 --ip <ip>", "Video File receiving consumer "
  method_option :ip, type: :string, default: '127.0.0.1'
  def consumer4
    require File.expand_path('../video_transfer_revisited/consumer', __FILE__)
    consumer = DAAS::Consumer.new(options)
    consumer.run    
 	end

  desc "producer5 --ip <ip> --in <filename> --out <filename> --dur <seconds>", "Transcoding producer"
  method_option :ip, type: :string, default: '127.0.0.1'
  method_option :in, type: :string
  method_option :out, type: :string
  method_option :dur, type: :numeric, default: 60
  method_option :pro, type: :string, default: 'profile1'
  def producer5
    producer = DAAS::Transcoding::Producer.new(options)
    producer.run    
  end

  desc "consumer5 --ip <ip>", "Transcoding consumer"
  method_option :ip, type: :string, default: '127.0.0.1'
  def consumer5
    consumer = DAAS::Transcoding::Consumer.new(options)
    consumer.run        
  end
end
