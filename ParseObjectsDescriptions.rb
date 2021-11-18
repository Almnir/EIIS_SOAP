require 'nokogiri'

class ParseObjectsDescriptions
    def ParseObjectsList(xml)
        doc = Nokogiri::Slop(xml)
        all_tables = ""
        all_descriptions = ""
        doc.list.object.each do |object|
            all_tables += ParseObjectToCreateString(object)
            all_desc = ParseObjectToAddDescriptionsString(object)
            all_descriptions += all_desc[0]
            all_descriptions += "----------------------------------------"
            all_descriptions += all_desc[1]
            all_descriptions += "--======================================"
        end
        [all_tables,all_descriptions]
    end

    def ParseObjectToAddDescriptionsString(object)
        table = object.attributes.values.select {|x| x.name == "code"}[0].value.split(".")
        # если это EIIS.AKNDPP2.SIGN_STATUSES, то берём и сливаем два последних чтобы получилось EIIS.AKNDPP2_SIGN_STATUSES в итоге
        table_name = ""
        if table.length > 2 then
            table_name = table.drop(1).take(2).join("_")
        else
            table_name = table.last
        end    
        table_desc = object.attributes.values.select {|x| x.name == "name"}[0].value
        table_description = %Q[
            EXEC sys.sp_addextendedproperty   
            @name = N'MS_Description',   
            @value = N'#{table_desc}',   
            @level0type = N'SCHEMA', @level0name = 'eiis',  
            @level1type = N'TABLE',  @level1name = '#{table_name}';  
        ]
        column_descriptions = ""
        object.elements.each do |element|
            column_name = element.attributes.values.select {|x| x.name == "code"}[0].value
            if element.name == "reference"
                col_desc = element.attributes.values.select {|x| x.name == "object"}[0].value
            else
                col_desc = element.attributes.values.select {|x| x.name == "name"}[0].value
            end
            column_description = %Q[
                EXEC sp_addextendedproperty   
                @name = N'#{column_name}', 
                @value = '#{col_desc}',  
                @level0type = N'Schema', @level0name = 'eiis',  
                @level1type = N'Table', @level1name = '#{table_name}',   
                @level2type = N'Column',@level2name = '#{column_name}';  
            ]
            column_descriptions += column_description
            # column_descriptions += "\n"
        end
        [table_description, column_descriptions]
    end

    def ParseObjectToCreateString(object)
        create_table_string = ""
        # берём последнее слово из тех, что через точку в названии объекта EIIS.EDUPROGRAMS - EDUPROGRAMS
        table = object.attributes.values.select {|x| x.name == "code"}[0].value.split(".")
        # если это EIIS.AKNDPP2.SIGN_STATUSES, то берём и сливаем два последних чтобы получилось EIIS.AKNDPP2_SIGN_STATUSES в итоге
        table_name = ""
        if table.length > 2 then
            table_name = table.drop(1).take(2).join("_")
        else
            table_name = table.last
        end    
        create_table_string += "CREATE TABLE [eiis].[#{table_name}](\n"
        column_strings = ""
        object.elements.each do |element|
            column_name = element.attributes['code'].value
            # проходим по всем элементам
            case element.name
            when "primary", "reference"
                column_type = "[nvarchar](max) NULL"
            when "column"
                col_int_type = element.attributes['type'].value
                case col_int_type.to_i
                when 5
                    column_type = "[datetime] NULL"
                when 1
                    column_type = "[bit] NULL"
                when 2
                    column_type = "[int] NULL"
                when 6
                    column_type = "[nvarchar](max) NULL"
                else
                    column_type = "[nvarchar](max) NULL"
                end
            end
            column_strings += " [#{column_name}] #{column_type},\n"
        end
        # удаляем последнюю запятую, она лишняя
        column_strings = column_strings[0...column_strings.rindex(",")]
        create_table_string += column_strings
        create_table_string += ");\n"
    end
end
