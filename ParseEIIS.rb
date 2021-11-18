require 'nokogiri'
require 'pp'
require 'date'
require 'sequel'
require 'tiny_tds'

class ParseEIIS

    def initialize
        @db_connection_params = {
            :host => '10.0.18.3',
            :database => 'esrp_prod',
            :user => 'ra',
            :password => '',
            :timeout => 300
        }
        # @db_connection_params = {
        #     :dataserver => 'FBS-SQL\R2',
        #     :user => 'esrp_prod',
        #     :timeout => 300
        # }        
    end

    def InsertParsedTable(xml, table_name)
        columns, import_data = ParseSimple(xml)
        db = Sequel.tinytds(@db_connection_params)
		target = db.from{eiis[table_name.to_sym]}
        import_data.each_slice(900) do |slice|
            target.import(columns, slice)
        end
        db.disconnect
    end

    # simple parsing
    def ParseSimple(xml)
        doc = Nokogiri::Slop(xml)
    
        # это данные
        result = []
        # это столбцы
        codes = []
        doc.object.row.each do |row|
            # заполняем данные в массив
            result << row.elements.children.map {|x| x.content.delete("\n")}
            # если столбцы не заполнены, заполняем один раз, этого достаточно т.к. все столбцы одинаковы для всех строк
            if codes.empty?
                row.elements.each do |element|
                    codes << element.attributes.select {|x| x == "code"}.values[0].value.intern
                end
            end
        end
        result.map! do |x|
            x.map! do |y|
                if y == "False" then
                    0
                elsif y == "True" then
                    1
                elsif y == "NULL" then
                    nil
                else
                    y
                end
            end
        end
        [codes, result]
    end 

    # parse and insert EIIS.FOUNDER_TYPES
    def InsertFounderTypes(xml)
        import_data = ParseSimple(xml)
        db = Sequel.tinytds(@db_connection_params)
        import_data.each_slice(900) do |slice|
            db[:FOUNDER_TYPES].import([:ID,
            :NAME,
            :CODE,
            :NOT_TRUE], slice)
        end
        db.disconnect
    end
    
    # parse and insert
    def InsertFounders(xml)
        import_data = ParseSimple(xml)
        db = Sequel.tinytds(@db_connection_params)
        import_data.each_slice(900) do |slice|
            db[:SchoolFounders].import([:ID,
            :TYPE_FK,
            :ORGANIZATION_FULLNAME,
            :ORGANIZATION_SHORTNAME,
            :LASTNAME,
            :FIRSTNAME,
            :PATRONYMIC,
            :PHONES,
            :FAXES,
            :EMAILS,
            :OGRN,
            :INN,
            :KPP,
            :L_ADDRESS,
            :L_ADDRESS_COUNTRY_FK,
            :L_ADDRESS_REGION_FK,
            :L_ADDRESS_DISTRICT,
            :L_ADDRESS_TOWN,
            :L_ADDRESS_STREET,
            :L_ADDRESS_HOUSE_NUMBER,
            :L_ADDRESS_POSTAL_CODE,
            :P_ADDRESS,
            :P_ADDRESS_COUNTRY_FK,
            :P_ADDRESS_REGION_FK,
            :P_ADDRESS_DISTRICT,
            :P_ADDRESS_TOWN,
            :P_ADDRESS_STREET,
            :P_ADDRESS_HOUSE_NUMBER,
            :P_ADDRESS_POSTAL_CODE], slice)
        end
        db.disconnect
    end

    # parse and insert
    def InsertSchools(xml)
        import_data = ParseSimple(xml)
        db = Sequel.tinytds(@db_connection_params)
        import_data.each_slice(900) do |slice|
            db[:Schools].import([:ID,
            :TYPE_FK,
            :ORGANIZATION_FULLNAME,
            :ORGANIZATION_SHORTNAME,
            :LASTNAME,
            :FIRSTNAME,
            :PATRONYMIC,
            :PHONES,
            :FAXES,
            :EMAILS,
            :OGRN,
            :INN,
            :KPP,
            :L_ADDRESS,
            :L_ADDRESS_COUNTRY_FK,
            :L_ADDRESS_REGION_FK,
            :L_ADDRESS_DISTRICT,
            :L_ADDRESS_TOWN,
            :L_ADDRESS_STREET,
            :L_ADDRESS_HOUSE_NUMBER,
            :L_ADDRESS_POSTAL_CODE,
            :P_ADDRESS,
            :P_ADDRESS_COUNTRY_FK,
            :P_ADDRESS_REGION_FK,
            :P_ADDRESS_DISTRICT,
            :P_ADDRESS_TOWN,
            :P_ADDRESS_STREET,
            :P_ADDRESS_HOUSE_NUMBER,
            :P_ADDRESS_POSTAL_CODE], slice)
        end
        db.disconnect
    end

    # EIIS.LICENSED_PROGRAMS
    def ParseLicensedPrograms(xml)
        doc = Nokogiri::Slop(xml)
        sqltext = ""
        doc.object.row.each do |x| 
            sqltext += %Q[insert into dbo.LicensedPrograms
                values(
                '#{x.primary.content.upcase}', 
                '#{x.reference("[@code='REGION_LOCATION']").content.upcase}',
                '#{x.reference("[@code='LICENSE_APPFK']").content.upcase}',
                '#{x.reference("[@code='EDUPROGRAMFK']").content.upcase}',
                '#{x.column("[@code='CODE']").content}',
                '#{x.column("[@code='NAME']").content}',
                '#{x.reference("[@code='EDULEVELFK']").content.upcase}',
                '#{x.reference("[@code='EDUPROGRAM_TYPEFK']").content.upcase}',
                '#{x.column("[@code='PERIOD']").content}',
                '#{x.column("[@code='QUALIFICATIONCODE']").content}',
                '#{x.column("[@code='QUALIFICATIONNAME']").content}',
                '#{x.column("[@code='QUALIFICATIONGRADE']").content}',
                '#{x.reference("[@code='LICENSE_STATFK']").content.upcase}',
                '#{x.column("[@code='OKSO']").content}',
                '#{x.column("[@code='STANDARD_TYPE']").content}',
                #{x.column("[@code='SYS_STATE']").content.to_i},
                '#{if x.column("[@code='SYS_CREATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_CREATED']").content).to_s end}',
                '#{if x.column("[@code='SYS_UPDATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_UPDATED']").content) end}',
                '#{x.column("[@code='NEW_EDUPROGRAMFK']").content}'
                ); 
            ]
            sqltext += "\n"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end

    # EIIS.SCHOOLS -> dbo.Schools
    def ParseSchools(xml)
        docxmlSchools = Nokogiri::Slop(xml)
        sqltext = ""
        docxmlSchools.object.row.each do |x| 
            sqltext += %Q[insert into dbo.Schools 
                values(
                '#{x.primary.content.upcase}', 
                '#{x.column("[@code='ISLOD_GUID']").content.upcase}',
                '#{x.column("[@code='NAME']").content}',
                '#{x.column("[@code='SHORTNAME']").content}',
                '#{x.column("[@code='REGULARNAME']").content}',
                '#{x.reference("[@code='SCHOOL_PROPERTYFK']").content.upcase}',
                '#{x.reference("[@code='SCHOOL_TYPEFK']").content.upcase}',
                '#{x.reference("[@code='SCHOOL_KINDFK']").content.upcase}',
                '#{x.reference("[@code='SCHOOL_CATEGORYFK']").content.upcase}',
                #{if x.column("[@code='BRANCH']").content == 'False' || x.column("[@code='BRANCH']").content == 'NULL' then 0 else 1 end},
                '#{x.reference("[@code='PARENTFK']").content.upcase}',
                #{if x.column("[@code='HASMILITARYDEPARTMENT']").content == 'False' || x.column("[@code='HASMILITARYDEPARTMENT']").content == 'NULL' then 0 else 1 end},
                #{if x.column("[@code='HASHOSTEL']").content == 'False' || x.column("[@code='HASHOSTEL']").content == 'NULL' then 0 else 1 end},
                #{x.column("[@code='HOSTELCAPACITY']").content.to_i},
                #{if x.column("[@code='HASHOSTELFORENTRANTS']").content == 'False' || x.column("[@code='HASHOSTELFORENTRANTS']").content == 'NULL' then 0 else 1 end},
                '#{x.column("[@code='LAW_ADDRESS']").content}',
                '#{x.column("[@code='LAW_POST_INDEX']").content}',
                '#{x.column("[@code='LAW_COUNTRYFK']").content}',
                '#{x.reference("[@code='LAW_REGIONFK']").content.upcase}',
                '#{x.reference("[@code='LAW_TOWNTYPEFK']").content.upcase}',
                '#{x.column("[@code='LAW_CITY_NAME']").content}',
                '#{x.column("[@code='LAW_STREET']").content}',
                '#{x.column("[@code='LAW_HOUSE']").content}',
                '#{x.column("[@code='LAW_OFFICE']").content}',
                '#{x.column("[@code='ADDRESS']").content}',
                '#{x.column("[@code='POST_INDEX']").content}',
                '#{x.column("[@code='COUNTRYFK']").content}',
                '#{x.reference("[@code='REGIONFK']").content.upcase}',
                '#{x.reference("[@code='TOWNTYPEFK']").content.upcase}',
                '#{x.column("[@code='TOWN_NAME']").content}',
                '#{x.column("[@code='STREET']").content}',
                '#{x.column("[@code='HOUSE']").content}',
                '#{x.column("[@code='OFFICE']").content}',
                '#{x.column("[@code='PHONES']").content}',
                '#{x.column("[@code='FAXS']").content}',
                '#{x.column("[@code='MAILS']").content}',
                '#{x.column("[@code='WWW']").content}',
                '#{x.column("[@code='GOSREGNUM']").content}',
                '#{x.column("[@code='INN']").content}',
                '#{x.column("[@code='KPP']").content}',
                '#{x.column("[@code='CHARGEPOSITION']").content}',
                '#{x.column("[@code='CHARGEFIO']").content}',
                '#{x.column("[@code='CONTACT_FIRST_NAME']").content}',
                '#{x.column("[@code='CONTACT_SECOND_NAME']").content}',
                '#{x.column("[@code='CONTACT_LAST_NAME']").content}',
                #{x.column("[@code='STUDENTS_COUNT']").content.to_i},
                #{x.column("[@code='SCHOOLLEAVER_COUNT']").content.to_i},
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                #{if x.column("[@code='OUTDATED']").content == 'False' || x.column("[@code='OUTDATED']").content == 'NULL' then 0 else 1 end},
                '#{x.reference("[@code='EX_SCHOOLFK']").content.upcase}',
                #{x.column("[@code='SYS_STATE']").content.to_i},
                '#{if x.column("[@code='SYS_CREATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_CREATED']").content).to_s end}',
                '#{if x.column("[@code='SYS_UPDATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_UPDATED']").content) end}',
                '#{x.column("[@code='IMPL_ADDR']").content}',
                '#{x.column("[@code='ADDRESS2']").content}',
                '#{x.column("[@code='MON_ID']").content}',
                '#{x.reference("[@code='STATUS_FK']").content.upcase}',
                '#{x.column("[@code='FOUNDERS']").content}',
                '#{x.column("[@code='INSIDE_SCHOOL_FK']").content}',
                '#{x.column("[@code='OUTSIDE_SCHOOL_FK']").content}',
                '#{x.column("[@code='RENAME']").content}',
                #{if x.column("[@code='ISSTRONG']").content == 'False' || x.column("[@code='ISSTRONG']").content == 'NULL' then 0 else 1 end},
                #{if x.column("[@code='ISRELIGION']").content == 'False' || x.column("[@code='ISRELIGION']").content == 'NULL' then 0 else 1 end},
                '#{x.column("[@code='GA_GUID']").content}'
                ); 
            ]
            sqltext += "\n"
            # puts sqltext
            # puts "----------------------------------------"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end

    # EIIS.SCHOOL_STATUSES -> dbo.SchoolStatuses
    def ParseSchoolStatuses(xml)
        docxmlSchoolStatuses = Nokogiri::Slop(xml)
        sqltext = ""
        docxmlSchoolStatuses.object.row.each do |x| 
            sqltext += %Q[insert into dbo.SchoolStatuses
                values(
                '#{x.primary.content.upcase}', 
                '#{x.column("[@code='NAME']").content}',
                #{if x.column("[@code='NOT_TRUE']").content == 'False' || x.column("[@code='NOT_TRUE']").content == 'NULL' then 0 else 1 end},
                #{x.column("[@code='CODE']").content.to_i}
                ); 
            ]
            sqltext += "\n"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end

    # EIIS.FED_OKR
    def ParseSchoolFedOkr(xml)
        docxmlFedOkr = Nokogiri::Slop(xml)
        sqltext = ""
        docxmlFedOkr.object.row.each do |x| 
            sqltext += %Q[insert into dbo.SchoolFedOkr
                values(
                '#{x.primary.content.upcase}', 
                '#{x.column("[@code='NAME']").content}',
                '#{x.column("[@code='SHORTNAME']").content}',
                #{if x.column("[@code='NOT_TRUE']").content == 'False' || x.column("[@code='NOT_TRUE']").content == 'NULL' then 0 else 1 end}
                ); 
            ]
            sqltext += "\n"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end
    
    # EIIS.SCHOOLPROPERTIES
    def ParseSchoolProperties(xml)
        docxmlSchoolProperties = Nokogiri::Slop(xml)
        sqltext = ""
        docxmlSchoolProperties.object.row.each do |x| 
            sqltext += %Q[insert into dbo.SchoolProperties
                values(
                '#{x.primary.content.upcase}', 
                #{x.column("[@code='CODE']").content.to_i},
                '#{x.column("[@code='NAME']").content}',
                #{if x.column("[@code='IS_STATE']").content == 'False' || x.column("[@code='IS_STATE']").content == 'NULL' then 0 else 1 end},
                #{if x.column("[@code='NOT_TRUE']").content == 'False' || x.column("[@code='NOT_TRUE']").content == 'NULL' then 0 else 1 end},
                '#{x.reference("[@code='SCHOOLPROPERTYFK']").content.upcase}',
                #{if x.column("[@code='NOT_TRUE_FOR_AKNDPP']").content == 'False' || x.column("[@code='NOT_TRUE_FOR_AKNDPP']").content == 'NULL' then 0 else 1 end}
                ); 
            ]
            sqltext += "\n"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end

    # EIIS.REGIONS
    def ParseSchoolRegions(xml)
        docxmlSchoolRegions = Nokogiri::Slop(xml)
        sqltext = ""
        docxmlSchoolRegions.object.row.each do |x| 
            sqltext += %Q[insert into dbo.SchoolRegions
                values(
                '#{x.primary.content.upcase}', 
                '#{x.column("[@code='NAME']").content}',
                '#{x.reference("[@code='FED_OKR_FK']").content.upcase}',
                #{x.column("[@code='SYS_STATE']").content.to_i},
                '#{if x.column("[@code='SYS_CREATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_CREATED']").content).to_s end}',
                '#{if x.column("[@code='SYS_UPDATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_UPDATED']").content) end}',
                #{x.column("[@code='REGION']").content.to_i},
                #{if x.column("[@code='NOT_TRUE']").content == 'False' || x.column("[@code='NOT_TRUE']").content == 'NULL' then 0 else 1 end}
                ); 
            ]
            sqltext += "\n"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end

    # EIIS.SCHOOL_KINDS
    def ParseSchoolKinds(xml)
        docxmlSchoolKind = Nokogiri::Slop(xml)
        sqltext = ""
        docxmlSchoolKind.object.row.each do |x| 
            sqltext += %Q[insert into dbo.SchoolKinds 
                values(
                '#{x.primary.content.upcase}', 
                #{x.column("[@code='CODE']").content},
                '#{x.column("[@code='NAME']").content}',
                '#{x.reference("[@code='SCHOOL_TYPEFK']").content.upcase}',
                #{x.column("[@code='SYS_STATE']").content.to_i},
                '#{if x.column("[@code='SYS_CREATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_CREATED']").content).to_s end}',
                '#{if x.column("[@code='SYS_UPDATED']").content == "NULL" then 'NULL' else Date.parse(x.column("[@code='SYS_UPDATED']").content) end}',
                #{if x.column("[@code='NOT_TRUE']").content == 'False' || x.column("[@code='NOT_TRUE']").content == 'NULL' then 0 else 1 end},
                #{if x.column("[@code='NOT_TRUE_FOR_AKNDPP']").content == 'False' || x.column("[@code='NOT_TRUE']").content == 'NULL' then 0 else 1 end}
                ); 
            ]
            sqltext += "\n"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end

    # EIIS.SCHOOL_TYPES
    def ParseSchoolTypes(xml)
        docSchoolType = Nokogiri::Slop(xml)
        sqltext = ""
        docSchoolType.object.row.each do |x|
            sqltext += %Q[insert into dbo.SchoolTypes 
                values(
                '#{x.primary.content.upcase}', 
                '#{x.column("[@code='NAME']").content}',
                #{x.column("[@code='SCHOOL_SUBTYPECODE']").content},
                #{if x.column("[@code='NOT_TRUE']").content == 'False' || x.column("[@code='NOT_TRUE']").content == 'NULL' then 0 else 1 end}
                );
            ]
            sqltext += "\n"
        end
        sqltext = sqltext.gsub("'NULL'","NULL")
        return sqltext
    end

end