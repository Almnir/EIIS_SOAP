require 'savon'
require 'json'
require 'nokogiri'

class EIIS
  attr_accessor :session_id
  attr_accessor :object_codes
  attr_accessor :package_ids

  def initialize
    @eiis_wsdl = "http://eiis-production.srvdev.ru/integrationservice/baseservice.asmx?WSDL"
    @session_id = ""
    @client = Savon.client(
      :wsdl => @eiis_wsdl,
      :unwrap => true,
      :pretty_print_xml => true,
      :env_namespace => :s,
      :open_timeout => 10,
      :read_timeout => 10,
      :convert_request_keys_to => :lower_camelcase,
      :log => false
    )
    @object_codes = []
    @package_ids = []
    puts("Creating SOAP client for: " + @eiis_wsdl)
  end

  def authorize(login, password)
    begin
      response = @client.call(:get_session_id, message: { login: login, password: password })
      doc = Nokogiri::XML(response.body.values[0][:get_session_id_result])
      @session_id = doc.at('session')['id']
      return response
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  def get_objects(include_fields)
    begin
      msg = { session_id: @session_id, fields_include: include_fields }
      # puts(msg)
      response = @client.call(:get_object_list, message: msg)
      return response.body.values[0][:get_object_list_result]
      # puts(response.body.values)
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  def parse_object_codes(objects)
    doc = Nokogiri::XML(objects)
    doc.xpath('//list/object/@code').each do |o|
      @object_codes << o.value
    end
  end

  def store_package_id(package_id)
    @package_ids << package_id
  end

  def create_package(object_code, history_create=false, document_include=false, filter="")
    if @object_codes.empty?
      puts "No codes available, please get objects by 'objects' command!"
      return nil
    end
    puts "Code value #{@object_codes[object_code.to_i]}"
    begin
      msg = { session_id: @session_id, object_code: @object_codes[object_code.to_i], history_create: history_create, document_include: document_include, filter: filter}
      # puts(msg)
      response = @client.call(:create_package, message: msg)
      # puts(response)
      doc = Nokogiri::XML(response.body.values[0][:create_package_result])
      package_id = doc.at('package')['id']
      return package_id
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  def get_package_meta(package_index)
    if @package_ids.empty?
      puts "No ids available, please create packages by 'create [Number]' command!"
      return nil
    end
    puts "Package metadata for #{@package_ids[package_index.to_i]}"    
    begin
      msg = { session_id: @session_id, package_id: @package_ids[package_index.to_i] }
      response = @client.call(:get_package_meta, message: msg)
      return response.body.values[0][:get_package_meta_result]
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  def serve
    print "login:"
    login = gets.chomp
    print "password:"
    password = gets.chomp
    auth = authorize(login, password)
    if auth != nil
      puts("Session ID set to #{@session_id}")
    else
      puts("Authorization failed.")
      return
    end
    loop do 
      puts "Please, master of EIIS, order your command:"
      cmd = gets.chomp
      case cmd
      when "commands"
         puts($client.operations)
      when "objects"
        objects = get_objects(false)
        if objects != nil
          pp objects
          parse_object_codes(objects)
        end
      when "print_codes"
        @object_codes.each_with_index do |code, index|
          puts "#{index}: #{code}"
        end
      when "print_packages"
        @package_ids.each_with_index do |id, index|
          puts "#{index}: #{id}"
        end
      when /^create (\d+)$/
        response = create_package($1)
        if response != nil
          pp "Package created with Package_id = #{response}"
          store_package_id(response)
        end
      when /^meta (\d+)$/
        response = get_package_meta($1)
        if response != nil
          pp response
        end
      when "session"
        puts @session_id
      when "exit"
        puts "Service is closing."
        break
      end 
    end
  end
end

eiis = EIIS.new
eiis.serve