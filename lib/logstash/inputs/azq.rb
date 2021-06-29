# encoding: utf-8
require "logstash/inputs/base"
require "stud/interval"
require "socket" # for Socket.gethostname
require "azure/storage/queue"

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Azq < LogStash::Inputs::Base
  config_name "azq"
  default :codec, "json"

  # name of the queuereq to pull from
  config :queue_name,           :validate => :string, :required => true
  config :id_field,             :validate => :string, :default  => "id"
  config :storage_account_name, :validate => :string, :required => true
  config :storage_access_key,   :validate => :string, :required => true


  public
  def register
    @host = Socket.gethostname
    @queue_client = Azure::Storage::Queue::QueueService.create(storage_account_name: @storage_account_name, storage_access_key: @storage_access_key )
   #print "thsi si a test"
  end # def register


  def handle_message(message, queue)
    @codec.decode(Base64.decode64(message.message_text)) do |event|
      add_asq_data(event, message)
      decorate(event)
      queue << event
    end
  end


  def run(queue)
    # we can abort the loop if stop? becomes true
    while !stop? 
      messages = @queue_client.list_messages(@queue_name, 30, { number_of_messages: 10} )

      messages.each do |message| 
        handle_message(message, queue)
        @queue_client.delete_message(@queue_name, message.id, message.pop_receipt)
      end
      
      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      Stud.stoppable_sleep(20) { stop? }
    end # loop
  end # def run

  def add_asq_data(event, message) 
    event.set(@id_field, message.id) if @id_field
  end

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end


  # def run(queue)
  #   # we can abort the loop if stop? becomes true
  #   while !stop?
  #     event = LogStash::Event.new("message" => @id_field, "host" => @host)
  #     decorate(event)
  #     queue << event
  #     # because the sleep interval can be big, when shutdown happens
  #     # we want to be able to abort the sleep
  #     # Stud.stoppable_sleep will frequently evaluate the given block
  #     # and abort the sleep(@interval) if the return value is true
  #     Stud.stoppable_sleep(@interval) { stop? }
  #   end # loop
  # end # def run


end # class LogStash::Inputs::Azq
