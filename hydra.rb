require 'json'
require './models/message_bag.rb'

#initialize my connection
BagManager = MessageBag.new()

class Hydra < Sinatra::Base

  get '/' do

    "ello there Poppet"
  end

  get '/:queue/status' do
    status = BagManager.status(params[:queue])
    content_type :json
    status.to_json
  end
    
  get '/:queue/message' do
    # return a message from the queue
    message = BagManager.get(params[:queue])
    if message.nil?
      status 204
    else 
      content_type :json
      status 200
      message.to_json
    end

  end

  delete '/:queue/message/:message' do
    BagManager.delete(params[:queue],params[:message])
    status 204
  end

  put '/:queue/message' do

    message_body = request.body.read
    key = BagManager.put(params[:queue], message_body)
    status 201
    content_type :json
    { :message_id => key }.to_json
  end

end
