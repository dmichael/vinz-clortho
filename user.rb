class User < Oracle
  set_table_name  "users"
  set_primary_key "user_id"

  
  set_package_name  "P_USERS"
  set_insert_method "insert_user" # required for save method to work
  set_update_method "update_user" # required for update method to work
  
  set_insert_args   "user_login",
                    "user_password", 
                    "user_expiration_date", 
                    "user_first_name", 
                    "user_middle_initial",
                    "user_last_name",
                    "user_email",
                    "user_phone",
                    "user_fax",
                    "user_type_code",
                    "user_activation_code",
                    "force_password_change",
                    "user_time_offset", 
                    "user_live",
                    "user_entry_id"  
                 
  set_update_args   "user_id",
                    "user_login",
                    "user_password", 
                    "user_expiration_date", 
                    "user_first_name", 
                    "user_middle_initial",
                    "user_last_name",
                    "user_email",
                    "user_phone",
                    "user_fax",
                    "user_type_code",
                    "user_activation_code",
                    "force_password_change",
                    "user_time_offset", 
                    "user_live",
                    "user_entry_id"  
  
  
  belongs_to :user_type, :foreign_key => "user_type_code"
    
end
