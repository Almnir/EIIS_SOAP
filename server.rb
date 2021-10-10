require 'sinatra'
require "sinatra/namespace"
require 'sinatra/base'
require 'savon'
require 'json'
require 'nokogiri'
require 'active_support/core_ext/hash'
require_relative 'eiis'

class App < Sinatra::Base
    register Sinatra::Namespace

    def initialize
        @eiis = EIIS.new
        # login, password
        auth = @eiis.authorize("", "")
        if auth != nil
            puts("Session ID set to #{@eiis.session_id}")
        else
            puts("Authorization failed.")
        end
        super
    end        

    get '/' do
        # 'Welcome!'
        redirect to('/api/v1/objects')
    end

    namespace '/api/v1' do
        before do
            content_type 'application/json'
        end
        after do
            puts response.status
        end

        helpers do
        end

        get '/objects' do
            objects = @eiis.get_objects(true)
            if objects != nil
                @eiis.parse_object_codes(objects)
                Hash.from_xml(Nokogiri::XML(objects).to_xml).to_json
            end
        end
    end
end

App.run!