require 'json'
require './models/message_bag.rb'

class Hydra < Sinatra::Base

  get '/' do

    "ello there Poppet"
  end

  get '/:queue/status' do
    status = @@bag_manager.status(params[:queue])
    content_type :json
    status.to_json
  end
    
  get '/:queue/message' do
    # return a message from the queue
    message = @@bag_manager.get(params[:queue])
    if message.nil?
      status 204
    else 
      content_type :json
      status 200
      message.to_json
    end

  end

  delete '/:queue/message/:message' do
    @@bag_manager.delete(params[:queue],params[:message])
    status 204
  end

  put '/:queue/message' do

    message_body = request.body.read
    key = @@bag_manager.put(params[:queue], message_body)
    status 201
    content_type :json
    { :message_id => key }.to_json
  end

  configure do
    @@bag_manager = MessageBag.new()
  end

end
