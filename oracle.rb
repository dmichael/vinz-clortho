# Oracle class for Rails for use with Oracle stored procedures.
# David M Michael 2006 while programming at GFI Group, NYC.

# This class overwrites some of ActiveRecords core functionality for use with Oracle stored procedures
# While this may seem like a bad idea, when using stored procedures, you dont really have access to the 
# native life cycle of ActiveRecord objects, including all those glorious callbacks. As this is developed,
# the functionality will be replaced

class Oracle < ActiveRecord::Base
  # the following may or may not be necessary - it seems to work without, but lets not risk it just yet
  # This corrects DateTime object type mapping
  OCI8::BindType::Mapping[OCI8::SQLT_DAT] = OCI8::BindType::DateTime
  OCI8::BindType::Mapping[OCI8::SQLT_TIMESTAMP] = OCI8::BindType::DateTime 
  
  $connection = ActiveRecord::Base.connection.raw_connection
  # This instance var is used for save and update messages
  attr_accessor :messages
  attr_accessor :oracle_errors
  attr_accessor :out

  # Stored procedure methods
  cattr_accessor :insert_method, :update_method, :delete_method
  # store of the arguments, in the order needed

  
  @@user_arguments_cache = {}
  
  # Redefining save() bypasses all callbacks in the active record life cycle
  # which is a real shame, but some of this can be restored with explicit calls 
  # This method IS copied to the object instance... and thus is an instance method
  def save(options = {})
    # default autocommit is TRUE
    autocommit = (options.nil? or options[:autocommit].nil?)? true : options[:autocommit]
    
    # transform the String values to their values in the object (effectively calling the String on the object) 
    insert_args_temp = []
    self.class.insert_args.each_with_index { |a, i| 
      logger.debug "self.#{a}" << "-" << eval("self.#{a}").to_s
      insert_args_temp[i] = eval("self.#{a}") 
      if (insert_args_temp[i].to_s.empty?) 
        eval("self.#{a} = nil") 
      end
    }
    before_validation
    if valid?  
      after_validation
      before_save    
      out = Oracle.execute_procedure(self.class.package_name, self.class.insert_method, {:in => insert_args_temp, :autocommit => autocommit})  
      after_save
      @message = out[:message]
      @oracle_errors = out[:message]
      @out = out[:outvars]
      return ((out[:error])? false : [true, out])
    else
      after_validation
      @messages = errors.full_messages.join(", ")
      
      return false
    end
  end
  
  def update(options = {})
    # default autocommit is TRUE
    if !options.nil?
      autocommit = (options[:autocommit].nil?)? true : options[:autocommit]
    end
    # transform the String values to their values in the object (effectively calling the String on the object) 
    update_args_temp = []
    self.class.update_args.each_with_index { |a, i| 
      update_args_temp[i] = eval("self.#{a}") 
      if (update_args_temp[i].to_s.empty?) 
        eval("self.#{a} = nil") 
      end
    }
    before_validation
    if valid?  
      after_validation
      before_update    
      out = Oracle.execute_procedure(self.class.package_name, self.class.update_method, {:in => update_args_temp, :autocommit => autocommit})  
      after_update
      @message = out[:message]
      @oracle_errors = out[:message]
      @out = out[:outvars]
      return ((out[:error])? false : true)
    else
      after_validation
      @messages = errors.full_messages.join(", ")
      
      return false
    end
  end

  # THIS APPEARS TO DUPLICATE THE SAVE, ONLY WITH THE UPDATE METHOD
 # def update(attributes)
 #   update_args_temp = []
 #   #logger.debug eval("#{self.class}.primary_key")
 #   # This expects a Hash
 #   @@update_args.each_with_index{ |a, i| 
 #     value = eval("attributes['#{a}']")
 #     update_args_temp[i] = value 
 #     # update the object's attribute so validation works
 #     (update_args_temp[i].empty?)? eval("self.#{a} = nil") : eval("self.#{a} = '#{value}'")
 #   } unless !@@update_args.is_a?(Hash)   
 #   before_validation
 #   if valid?    
 #     after_validation
 #     before_update
 #     out = Oracle.execute_procedure(@@package_name, @@update_method, {:in => update_args_temp})  
 #     after_update
 #     @messages = out[:message]
 #     return ((out[:error])? false : true)
 #   else
 #     
 #     after_validation
 #     @messages = errors.full_messages.join(", ")
 #     return false
 #   end
 # end

  
  # These method setters mimic Active Record's style
  # They should not be available to object instances - they are static/class methods
  class << self # Class methods
    
    # This is the least intuitive part of the configuration
    # You must specify the column names in the order the procedure is expecting them
    # These effectively will get called on the object as methods
    #def set_insert_args(*insert_args)
    #  self.insert_args = insert_args
    #end
    def define_attr_method_array(name, value)
      sing = class << self; self; end
      sing.class_eval "def #{name}; #{value.to_a.inspect}; end"
    end
    
    def define_attr_method_noalias(name, value=nil, &block)
      sing = class << self; self; end
      if block_given?
        sing.send :define_method, name, &block
      else
        # use eval instead of a block to work around a memory leak in dev
        # mode in fcgi
        sing.class_eval "def #{name}; #{value.to_s.inspect}; end"
      end
    end
    
    def set_package_name(value)
      define_attr_method_noalias :package_name, value
    end
    alias :package_name= :set_package_name
    
    def set_insert_method(value)
      define_attr_method_noalias :insert_method, value
    end
    alias :insert_method= :set_insert_method
    
    def set_update_method(value)
      define_attr_method_noalias :update_method, value
    end
    alias :update_method= :set_update_method
    
    def set_delete_method(value)
      define_attr_method_noalias :delete_method, value
    end
    alias :delete_method= :set_delete_method
    
    def set_insert_args(*value)
      define_attr_method_array("insert_args", value.to_a)
    end
    alias :insert_args= :set_insert_args
    
    def set_update_args(*value)
      define_attr_method_array("update_args", value.to_a)
    end
    alias :update_args= :set_update_args
  
    # Execute a stored procedure, explicitly binding parameters in Oracle
    def execute_procedure( package_name, object_name, options = {} )
      
      logger = RAILS_DEFAULT_LOGGER
      key = package_name + '_' + object_name
      recordset_return_type = !options[:recordset_return_type].nil? ? options[:recordset_return_type] : 'hash'
      
      if !options.nil?
      autocommit = (options[:autocommit].nil?)? true : options[:autocommit]
      end
      autocommit = !options[:autocommit].nil? ? options[:autocommit] : true
      # this is a costly method so we try to store the arguments in @user_arguments_cache
      get_arguments_from_stored_procedure( package_name, object_name )
      
      # Build the stored procedure inserting the appropriate arguments
      acopy = []
      @@user_arguments_cache[key].each_with_index{ |argument, x| acopy.push(":#{argument['ARGUMENT_NAME']}") }
      stored_procedure = "BEGIN #{package_name.upcase}.#{object_name.downcase}(#{acopy.join(', ').downcase}); END;"
      logger.debug "\n" << stored_procedure << "\n"
      
      # Setup the cursor so we can bind the params
      sql = $connection.parse(stored_procedure)
      
      y = 0
      # Loop through all the user arguments and bind the appropriate variable to them
      @@user_arguments_cache[key].each_with_index{ |argument, x|
        # Bind IN variables
        if argument['IN_OUT'] == 'IN'  
          field, datatype = oracle_data_type_conversion(options[:in][y], argument['DATA_TYPE'], argument['DATA_SCALE'])
          logger.debug "IN  sql.bind_param(:#{argument['ARGUMENT_NAME']}, #{field}) #{argument['DATA_TYPE']}"
          # This is here because the OCI driver figures out the bind type by the variable type...
          # except when the String is nil
          if (argument['DATA_TYPE'] == "VARCHAR2" || argument['DATA_TYPE'] == "CHAR") && field == ""
            sql.bind_param(":#{argument['ARGUMENT_NAME']}", "", String, 7) 
          else
            sql.bind_param(":#{argument['ARGUMENT_NAME']}", field) 
          end
          y = y + 1
        # Bind OUT variable for REF CURSOR
        elsif argument['IN_OUT'] == 'OUT' && argument['DATA_TYPE'] == 'REF CURSOR'
          logger.debug "OUT sql.bind_param(:#{argument['ARGUMENT_NAME']}, OCI8::Cursor) #{argument['DATA_TYPE']}"
          sql.bind_param(":#{argument['ARGUMENT_NAME']}", OCI8::Cursor)     
     
        # Bind OUT variables for everything else  
        elsif argument['IN_OUT'] == 'OUT' && argument['DATA_TYPE'] != 'REF CURSOR'
       
          field, datatype = oracle_data_type_conversion("", argument['DATA_TYPE'], argument['DATA_SCALE'])
          if datatype == String
            logger.debug "OUT sql.bind_param(:#{argument['ARGUMENT_NAME']}, nil, #{datatype}, 4000) #{argument['DATA_TYPE']})"
            sql.bind_param(":#{argument['ARGUMENT_NAME']}", nil, datatype, 7000)
          else
          logger.debug "OUT sql.bind_param(:#{argument['ARGUMENT_NAME']}, nil, #{datatype}) #{argument['DATA_TYPE']})"
            sql.bind_param(":#{argument['ARGUMENT_NAME']}", nil, datatype)
          end      
        end
      }
      
      # Now that everything is bound and whatnot, 
      # lets execute and commit the transaction
      begin
        $connection.autocommit = false
        sql.exec()  
        @outvars = {}
        #logger.debug "\n" << "Transaction successful"
        # PROCESS THE OUT VARS
        @@user_arguments_cache[key].each_with_index{ |argument, x|
          # Make sure argument is of TYPE = REF CURSOR
          if argument['IN_OUT'] == 'OUT' && argument['DATA_TYPE'] == 'REF CURSOR' 
           cursor = eval("sql[':#{argument['ARGUMENT_NAME']}']")    
            @recordset, @column_names = pack_cursor(cursor, :return => recordset_return_type)
          end
          
          if( argument['IN_OUT'] == 'OUT' && argument['DATA_TYPE'] != 'REF CURSOR' )
            begin
              @outvars[argument['ARGUMENT_NAME']] = eval("sql[':#{argument['ARGUMENT_NAME']}']")
            rescue Exception => e
              logger.debug("Oracle exception caught: " + e) 
            end
          end
        }       
        # Commit and close the cursor
        sql.close()
        
        $connection.commit() unless autocommit == false
          
              
        return { :recordset => @recordset, :column_names => @column_names, :outvars => @outvars, :message => 'Transaction successful', :error => false }
        
      # Catch Oracle errors and try to give the user a message that makes sense
      rescue OCIException => e
        logger.warn "\n" << "Oracle error code: #{e}"
        logger.warn "Oracle error message: #{e.message}"
        #message = oracle_error_codes(e)
        #unless message.nil?
        #  message =  "#{message}"
        #else
          message =  "#{e.message}"
        #end
        # Dont return anything
    
        return { :error => true, :message => message }  
      end
       
    end
 
  
  
    # Gets the expected user arguments for the stored procedure from Oracle. Only 10g
    def get_arguments_from_stored_procedure( package_name, object_name )
      key = package_name + '_' + object_name
      if @@user_arguments_cache[key].nil?
          #RAILS_DEFAULT_LOGGER.info "\n" << "GETTING ARGUMENTS FOR THE STORED PROCEDURE AND CACHING!"
          select_statement = "SELECT argument_name, data_type, data_length, data_precision, data_scale, in_out, overload 
                              FROM user_arguments WHERE object_name = '#{object_name.upcase}' 
                              AND package_name = '#{package_name.upcase}' 
                              ORDER BY POSITION"
                              
         #RAILS_DEFAULT_LOGGER.info "\n" << select_statement << "\n"
          user_arguments = recordset_from_plsql(select_statement) 
          @@user_arguments_cache[key] = user_arguments
        else
          #RAILS_DEFAULT_LOGGER.info "\n" << "THE ARGUMENTS ARE CACHED"
        end
        return @@user_arguments_cache[key]
    end
    
    # Lookup of user readable commonly returned Oracle errors.  
    def oracle_error_codes(e)
      string =  case
                when e.code == 1400   : "At least one of the required fields is blank and cannot be. Please fill in required fields.\n" << e
                when e.code == 1407   : "At least one of the required fields is blank and cannot be. Please fill in required fields.\n" << e 
                when e.code == 20000  : "ERROR! This stored procedure is broken.\n" << e
                when e.code == 12899  : "One of your values is too large.\n" << e
                when e.code == 20024  : "Incorrect username or password.\n" << e
                when e.code == 20025  : "Incorrect username or password.\n" << e
                when e.code == 1459   : "Please fill in the required fields.\n" << e
                else nil
                end              
    end
    
    # Converts the input from the method call (usually a string) into the Ruby datatype that is expected by the OCI8 driver
    def oracle_data_type_conversion(in_var, data_type, data_scale)
              
              case
                when data_type == "VARCHAR2"
                  if in_var.nil? or in_var.empty?
                    in_var = ""
                  end
                  this_in_var = in_var.to_s
                  this_data_type = String
                  
                when data_type == "CHAR"
                  if in_var.nil? or in_var.empty?
                    in_var = ""
                  end
                  this_in_var = in_var.to_s
                  this_data_type = String
                  
                when data_type == "NUMBER"
                  if !data_scale.nil? and data_scale > 0
                   
                    this_in_var = in_var.to_f
                    this_data_type = Float
                  else
                    this_in_var = in_var.to_i
                    this_data_type = Fixnum
                  end
                  
                when data_type == "TIMESTAMP"
                  this_in_var = in_var
                  this_data_type = DateTime
                  
                when data_type == "DATE"
                  this_in_var = in_var
                  this_data_type = DateTime
                  
                else nil
              end  
              
      return this_in_var, this_data_type 
    end
    
    # Returns an Array of Hashs...
    # Each index in the array is one row from the DB resultset.
    # The Hash at each index contains [key, value] = [DB column name, Value of that column for the row]
    def recordset_from_plsql(sp)
      logger.debug "\n" << sp << "\n"
      cursor = $connection.exec(sp)
      recordset, = pack_cursor(cursor, :return => 'hash')
      return recordset
    end
    
    # Executes raw SQL statements against the Oracle connection.
    # Takes a parameter for returning a hash or an array.
    # NB: taking varargs as a predefined Hash, rather than something like "*options" allows address by key, rather than by index
    def exec_raw(sql, options = {})
      cursor = $connection.exec(sql)
      if(options[:return_hash])
        recordset, = pack_cursor(cursor, :return => "hash")
        return recordset
      else
        return_data = []
        while current_row = cursor.fetch()
          return_data.push(current_row)
        end
        return return_data
      end
    end
    
    # TODO: change the pack_cursor to 2 methods... cursor_to_array() and cursor_to_hash()
    # Utility function to pack the ref cursor result set into an Array of Hashs or of Arrays (!)
    def pack_cursor(cursor, options = {})
 
      recordset = []
      column_names = []
      var_cursor = cursor.get_col_names
      
      while current_row = cursor.fetch()    
        case options[:return]
          when 'hash'
            current_record = {}
            current_row.each_index{ |index|  
              current_record[var_cursor[index]] = current_row[index] 
              column_names[index] = var_cursor[index].split('_').join(' ')
            }
          when 'array'
            current_record = []
            current_row.each_index{ |index| 
              current_record[index] = current_row[index] 
              column_names[index] = var_cursor[index].split('_').join(' ')
            }  
        end
    
        recordset.push(current_record)
      end
  
      return recordset, column_names
    end
  
    
    
    #-----------------------------------------------------------------------
    # THIS IS LEGACY AND SHOULD PROBABABLY BE DELETED
    # ONLY USED FOR TABLES THAT ARE FORMATED
   
    def recordset_from_stored_procedure(sp)
     
      plsql = $connection.parse(sp)
      plsql.bind_param(':out', OCI8::Cursor)
      plsql.exec
  
      cursor = plsql[':out']
     
      
      recordset, = pack_cursor(cursor, :return => 'hash')
      plsql.close
      return recordset
    end
  
  end 
  
end

  