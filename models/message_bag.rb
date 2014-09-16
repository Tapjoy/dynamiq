require 'riak'
require 'uuid'

class MessageBag
  @@visible_time = 30
  @@host         = '127.0.0.1'

  def new()

    # doesn't work need to find mechanism of persisting clients across sinatra instances
    riak = Riak::Client.new(:nodes =>[
      {:@host => @@host }
    ])
    return self
  end

  def put(bag_name, message)
    riak = Riak::Client.new(:nodes =>[
      {:@host => @@host }
    ])
    #create a bag object
    bag = riak.bucket(bag_name)
    #create a uuid
    uuid = UUID.new
    message_id = uuid.generate
    # create an object out of the passed message and store the message
    message_obj = Riak::RObject.new(bag, message_id)
    message_obj.content_type = 'message'
    message_obj.indexes['id_bin'] = [message_id]
    message_obj.raw_data = message
    message_obj.store
    #return the message id
    message_obj.key
  end

  def delete(bag_name, id)
    riak = Riak::Client.new(:nodes =>[
      {:@host => @@host }
    ])
    #get the bag
    bag = riak.bucket(bag_name)
  
    #delete the message
    bag.delete(id)
  end
  
  def get(bag_name)
    riak = Riak::Client.new(:nodes =>[
      {:@host => @@host }
    ])
    #get the bag
    bag = riak.bucket(bag_name)
    #get the messages that have been read in the last 30 seconds
    puts @@visible_time
    message_inflight_ids = bag.get_index( 'inflight_int', (Time.now.to_i - @@visible_time)..Time.now.to_i)
    #get inflight+1 messages
    message_ids  = bag.get_index( 'id_bin', '0'..'z', max_results: (message_inflight_ids.count()+1))
    #remove any messages that are inflight
    message_ids  = message_ids - message_inflight_ids

    #check if we didn't get any messages back
    if message_ids.first.nil?
      return nil
    else
      #otherwise write the time that we started processing this message
      message_obj = bag.get(message_ids.first)
      message_obj.indexes['inflight_int'] = [ Time.now.to_i ]
      message_obj.store
    end
    
    #send back the message 
    return {
     :message_id => message_obj.key,
     :message    => message_obj.raw_data
    }
  end

  def status(bag_name)
    riak = Riak::Client.new(:nodes =>[
      {:@host => @@host }
    ])
    #initialize the status hash
    status = {}
    #get the queue
    bag = riak.bucket(bag_name)

    #get the messages visible
    message_ids  = bag.get_index( 'id_bin', '0'..'z')
    visible_count = message_ids.count()
    status['visible'] = visible_count
    #get the messages inflight
    message_inflight_ids = bag.get_index( 'inflight_int', (Time.now.to_i - 30)..Time.now.to_i)
    inflight_count = message_inflight_ids.count()
    
    status['inflight'] = inflight_count
    return status
  end
end
