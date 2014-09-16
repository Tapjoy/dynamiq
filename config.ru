require "rubygems"

require "bundler/setup"

require "sinatra"


require "./hydra"

set :run, false

set :raise_errors, true

run Hydra
