class Daas < Thor
  
  include Thor::Actions

  desc "producer1 --ip <ip>", "Start producer: for direct exchange test"
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

  desc "consumer1 --ip <ip>", "Start consumer: for direct exchange test"
  method_option :ip, type: :string, default: '127.0.0.1'
  def consumer1
    require File.expand_path('../basic_amqp/consumer', __FILE__)
    consumer = DAAS::Consumer.new(options)
    consumer.run    
	end

  desc "producer2 --ip <ip> --in <filename> --out <filename>", "Start producer for file trasfer test"
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

  desc "consumer2 --ip <ip> --sleep <sec>", "Start consumer for file trasfer test"
  method_option :ip, type: :string, default: '127.0.0.1'
  method_option :sleep, type: :numeric, default: 3
  def consumer2    
    require File.expand_path('../file_transfer/consumer', __FILE__)
    consumer = DAAS::Consumer.new(options)
    consumer.run
	end
end