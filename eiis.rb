require 'savon'
require 'json'
require 'nokogiri'
require 'active_support/core_ext/hash'
require_relative 'ParseEIIS.rb'
require 'benchmark'
require 'tty-reader'

class EIIS
  attr_accessor :session_id
  attr_accessor :object_codes

  def initialize
    @eiis_wsdl = "http://eiis-production.srvdev.ru/integrationservice/baseservice.asmx?WSDL"
    # @eiis_wsdl = "https://eiis.obrnadzor.gov.ru/IntegrationService/BaseService.asmx?WSDL"
    @primary_key_code = 'ID'
    @session_id = ""
    @client = Savon.client(
      :wsdl => @eiis_wsdl,
      :unwrap => true,
      :pretty_print_xml => true,
      :env_namespace => :s,
      :open_timeout => 200,
      :read_timeout => 200,
      :convert_request_keys_to => :lower_camelcase,
      :log => false
    )
    @object_codes = []
    @package_ids = {}
    @parseEIIS = ParseEIIS.new
    @tty_reader = TTY::Reader.new(track_history: true, history_cycle: true)
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

  ### получение списка объектов
  def get_objects(include_fields)
    begin
      msg = { session_id: @session_id, fields_include: include_fields }
      response = @client.call(:get_object_list, message: msg)
      return response.body.values[0][:get_object_list_result]
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

  ### получение метаданных объекта
  def get_object_meta(object_code)
    if @object_codes.empty?
      puts "No codes available, please get objects by 'objects' command!"
      return nil
    end
    puts "Code value #{@object_codes[object_code.to_i]}"
    begin
      msg = { session_id: @session_id, object_code: @object_codes[object_code.to_i] }
      response = @client.call(:get_object_meta, message: msg)
      return response.body.values[0][:get_object_meta_result]
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  ### получение метаданных документа
  ### не работает по непонятной причине, возвращает код 033
  def get_document_meta(object_code)
    if @object_codes.empty?
      puts "No codes available, please get objects by 'objects' command!"
      return nil
    end
    puts "Code value #{@object_codes[object_code.to_i]}"
    begin
      msg = { session_id: @session_id, object_code: @object_codes[object_code.to_i], primary_key: @primary_key_code }
      pp msg
      response = @client.call(:get_document_meta, message: msg)
      pp response
      return response.body.values[0][:get_document_data_response]
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  def store_package_id(package_id, object_code)
    # забираем метаданные объекта чтобы название вытащить
    response = get_object_meta(object_code)
    if response != nil
      doc = Nokogiri::XML(response)
      object_name = doc.at('object')['code']
      @package_ids[package_id] = object_name
    end
  end

  ### создание пакета по индексу объекта
  ### остальные параметры интересны, но непонятно что делают
  def create_package(object_code, history_create=false, document_include=true, filter="")
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

  ### получить метаданные пакета по индексу пакета
  def get_package_meta(package_index)
    if @package_ids.empty?
      puts "No ids available, please create packages by 'create [Number]' command!"
      return nil
    end
    begin
      msg = { session_id: @session_id, package_id: @package_ids.keys[package_index.to_i] }
      response = @client.call(:get_package_meta, message: msg)
      return response.body.values[0][:get_package_meta_result]
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  ### Данные по пакету кусочками
  ### индекс, кусочек(наверное начинается с 1 кусочка)
  def get_package_data(package_index, part)
    if @package_ids.empty?
      puts "No ids available, please create packages by 'create [Number]' command!"
      return nil
    end
    begin
      msg = { session_id: @session_id, package_id: @package_ids.keys[package_index.to_i], part: part }
      response = @client.call(:get_package, message: msg)
      return response.body.values[0][:get_package_result]
    rescue Savon::HTTPError => error
      Logger.log error.http.code
      return nil
    end
  end

  def get_package_all(package_index)
    if @package_ids.empty?
      puts "No ids available, please create packages by 'create [Number]' command!"
      return nil
    end
    response = get_package_meta($1)
    if response != nil
      doc = Nokogiri::XML(response)
      pp doc
      capacity = doc.at('package')['capacity']
      pp capacity
    end
    all_data = ""
    # no threads
    # puts "No threads, single!!!!!!!!!!!!!!!!!!!!!!!"
    # Benchmark.bm do |x|
    #   x.report do
    #     (1..capacity.to_i).each do |part|
    #       all_data += get_package_data(package_index, part)
    #     end
    #   end
    # end
    File.delete("inserts.sql") if File.exists?("inserts.sql") 
    threads = []
    Benchmark.bm do |x|
      x.report do
        (1..capacity.to_i).each do |n|
              all_data = get_package_data(package_index, n)
              parse_xml_data_and_write_to_file(all_data, package_index)
        end
      end
    end
    set_ok(package_index)
  end

  def parse_xml_data_and_write_to_file(all_data, package_index)
    # парсим ответ
    case @package_ids.values[package_index.to_i]
      when "EIIS.REGIONS"
        sqltext = @parseEIIS.ParseSchoolRegions(all_data)
      when "EIIS.SCHOOLPROPERTIES"
        sqltext = @parseEIIS.ParseSchoolProperties(all_data)
      when "EIIS.FED_OKR"
        sqltext = @parseEIIS.ParseSchoolFedOkr(all_data)
      when "EIIS.SCHOOL_KINDS"
        sqltext = @parseEIIS.ParseSchoolKinds(all_data)
      when "EIIS.SCHOOL_TYPES"
        sqltext = @parseEIIS.ParseSchoolTypes(all_data)
      when "EIIS.SCHOOLS"
        sqltext = @parseEIIS.ParseSchools(all_data)
      when "EIIS.SCHOOL_STATUSES"
        sqltext = @parseEIIS.ParseSchoolStatuses(all_data)
      when "EIIS.LICENSED_PROGRAMS"
        sqltext = @parseEIIS.ParseLicensedPrograms(all_data)
      when "EIIS.FOUNDERS"
        sqltext = @parseEIIS.InsertFounders(all_data)
      when "EIIS.FOUNDER_TYPES"
        sqltext = @parseEIIS.InsertFounderTypes(all_data)
      else
        puts "No such parsing fascilities!"
        return 1
    end
    unless sqltext == nil && sqltext.empty?
      File.open("inserts.sql", "a") do |file|
        file << sqltext
      end
    end
    return 0
  end

  ### Подтверждение успешного получения кусочка данных пакета (не знаю зачем, но ладно)
  def set_ok(package_index)
    if @package_ids.empty?
      puts "No ids available, please create packages by 'create [Number]' command!"
      return nil
    end
    begin
      msg = { session_id: @session_id, package_id: @package_ids.keys[package_index.to_i] }
      response = @client.call(:set_ok, message: msg)
      return response.body
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
      objects = get_objects(true)
      if objects != nil
        pp objects
        # content = Hash.from_xml(Nokogiri::XML(objects).to_xml).to_json
        parse_object_codes(objects)
        @object_codes.each_with_index do |code, index|
          puts "#{index}: #{code}"
        end
      end
  else
      puts("Authorization failed.")
      return
    end
    @tty_reader.on(:keyctrl_x, :keyescape) do
      puts "Exiting..."
      exit
    end
    @tty_reader.on(:keyctrl_x, :keyescape) do
      puts "Exiting..."
      exit
    end
    loop do
      puts "Please, master of EIIS, order your command:"
      # cmd = gets.chomp
      cmd = @tty_reader.read_line("=> ")
      case cmd.strip
      when "commands"
        cmds = %q[
        exit - Quit EIIS server
        session - Print current session
        operations - Prints all EIIS WDSL entry points (not all of them are implemented yet)
        objects - Get all available objects from EIIS (should be the first command)
        print_codes - Print indexed list of objects (you need to know object index)
        obj_meta N - Get object metadata from object, with N as object index
        create N - Create package from object, with N as object index
        print_packages - Print indexed list of packages (you need to know package index)
        package_meta N - Get package metadata from package, with N as package index
        package_data N P - Get package data from package, with N as package index and P as part index (starts with 1 I guess)
        package_all N - Get all package data from package, with N as package index, writes to 'inserts.sql' file
        ]
        puts "#{cmds}"
      when "operations"
        puts(@client.operations)
      when "objects"
        objects = get_objects(true)
        if objects != nil
          pp objects
          # content = Hash.from_xml(Nokogiri::XML(objects).to_xml).to_json
          parse_object_codes(objects)
        end
      when "print_codes"
        @object_codes.each_with_index do |code, index|
          puts "#{index}: #{code}"
        end
      when "print_packages"
        @package_ids.each_with_index do |keyval, index|
          puts "#{index}: #{keyval.join(":")}"
        end
      when /^create (\d+)$/
        response = create_package($1)
        if response != nil
          pp "Package created with Package_id = #{response}"
          store_package_id(response, $1)
        end
      when /^package_meta (\d+)$/
        puts "Package metadata for #{@package_ids.values[$1.to_i]}"    
        response = get_package_meta($1)
        if response != nil
          pp response
        end
      when /^package_data (\d+) (\d+)$/
        puts "Package part data for #{@package_ids.values[$1.to_i]} part : #{$2.to_i}"
        response = get_package_data($1, $2)
        if response != nil
          pp response
          puts "Set Ok package data for #{@package_ids.values[$1.to_i]}"
          puts set_ok($1)
        end
      when /^package_all (\d+)$/
        puts "All package data for #{@package_ids.values[$1.to_i]}"
        response = get_package_all($1)
        puts "File saved!"
      when /^obj_meta (\d+)$/
        response = get_object_meta($1)
        if response != nil
          pp response
        end
      when /^doc_meta (\d+)$/
        response = get_document_meta($1)
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