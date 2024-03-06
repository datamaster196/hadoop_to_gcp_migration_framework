--DIM_ADDRESS
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_address` WHERE address_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_address` (
    address_adw_key
    ,raw_address_1_nm
    ,raw_address_2_nm
    ,raw_care_of_nm
    ,raw_city_nm
    ,raw_state_cd
    ,raw_postal_cd
    ,raw_postal_plus_4_cd
    ,raw_country_nm
    ,cleansed_address_1_nm
    ,cleansed_address_2_nm
    ,cleansed_care_of_nm
    ,cleansed_city_nm
    ,cleansed_state_cd
    ,cleansed_state_nm
    ,cleansed_postal_cd
    ,cleansed_postal_plus_4_cd
    ,cleansed_country_nm
    ,latitude_nbr
    ,longitude_nbr
    ,county_nm
    ,county_fips
    ,tm_zone_nm
    ,address_typ_cd
    ,address_valid_status_cd
    ,address_valid_typ
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                          --address_adw_key
    ,'UNKNOWN'                    --raw_address_1_nm
    ,'UNKNOWN'                    --raw_address_2_nm
    ,'UNKNOWN'                    --raw_care_of_nm
    ,'UNKNOWN'                    --raw_city_nm
    ,'UNKNOWN'                    --raw_state_cd
    ,'UNKNOWN'                    --raw_postal_cd
    ,'UNKNOWN'                    --raw_postal_plus_4_cd
    ,'UNKNOWN'                    --raw_country_nm
    ,'UNKNOWN'                    --cleansed_address_1_nm
    ,'UNKNOWN'                    --cleansed_address_2_nm
    ,'UNKNOWN'                    --cleansed_care_of_nm
    ,'UNKNOWN'                    --cleansed_city_nm
    ,'UNKNOWN'                    --cleansed_state_cd
    ,'UNKNOWN'                    --cleansed_state_nm
    ,'UNKNOWN'                    --cleansed_postal_cd
    ,'UNKNOWN'                    --cleansed_postal_plus_4_cd
    ,'UNKNOWN'                    --cleansed_country_nm
    ,0                            --latitude_nbr
    ,0                            --longitude_nbr
    ,'UNKNOWN'                    --county_nm
    ,'UNKNOWN'                    --county_fips
    ,'UNKNOWN'                    --tm_zone_nm
    ,'UNKNOWN'                    --address_typ_cd
    ,'UNKNOWN'                    --address_valid_status_cd
    ,'UNKNOWN'                    --address_valid_typ
    ,CURRENT_DATETIME             --integrate_insert_datetime
    ,{{ dag_run.id }}             --integrate_insert_batch_number
    ,CURRENT_DATETIME             --integrate_update_datetime
    ,{{ dag_run.id }}             --integrate_update_batch_number
);

--DIM_CONTACT_INFO
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info` WHERE contact_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info` (
     contact_adw_key
    ,contact_typ_cd
    ,contact_first_nm
    ,contact_middle_nm
    ,contact_last_nm
    ,contact_suffix_nm
    ,contact_title_nm
    ,contact_address_1_nm
    ,contact_address_2_nm
    ,contact_care_of_nm
    ,contact_city_nm
    ,contact_state_cd
    ,contact_state_nm
    ,contact_postal_cd
    ,contact_postal_plus_4_cd
    ,contact_country_nm
    ,contact_phone_nbr
    ,contact_phone_formatted_nbr
    ,contact_phone_extension_nbr
    ,contact_email_nm
    ,contact_gender_cd
    ,contact_gender_nm
    ,contact_birth_dt
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
     '-1'                         --contact_adw_key
    ,'UNKNOWN'                    --contact_typ_cd
    ,'UNKNOWN'                    --contact_first_nm
    ,'UNKNOWN'                    --contact_middle_nm
    ,'UNKNOWN'                    --contact_last_nm
    ,'UNKNOWN'                    --contact_suffix_nm
    ,'UNKNOWN'                    --contact_title_nm
    ,'UNKNOWN'                    --contact_address_1_nm
    ,'UNKNOWN'                    --contact_address_2_nm
    ,'UNKNOWN'                    --contact_care_of_nm
    ,'UNKNOWN'                    --contact_city_nm
    ,'UNKNOWN'                    --contact_state_cd
    ,'UNKNOWN'                    --contact_state_nm
    ,'UNKNOWN'                    --contact_postal_cd
    ,'UNKNOWN'                    --contact_postal_plus_4_cd
    ,'UNKNOWN'                    --contact_country_nm
    ,'UNKNOWN'                    --contact_phone_nbr
    ,'UNKNOWN'                    --contact_phone_formatted_nbr
    ,-1                           --contact_phone_extension_nbr
    ,'UNKNOWN'                    --contact_email_nm
    ,'UNKNOWN'                    --contact_gender_cd
    ,'UNKNOWN'                    --contact_gender_nm
    ,'1900-01-01'                 --contact_birth_dt
    ,'-1'                         --adw_row_hash
    ,CURRENT_DATETIME             --integrate_insert_datetime
    ,{{ dag_run.id }}             --integrate_insert_batch_number
    ,CURRENT_DATETIME             --integrate_update_datetime
    ,{{ dag_run.id }}             --integrate_update_batch_number
);


--DIM_EMAIL
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_email` WHERE email_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_email` (
    email_adw_key
    ,raw_email_nm
    ,cleansed_email_nm
    ,email_top_level_domain_nm
    ,email_domain_nm
    ,email_valid_status_cd
    ,email_valid_typ
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                             --email_adw_key
    ,'UNKNOWN'                       --raw_email_nm
    ,'UNKNOWN'                       --cleansed_email_nm
    ,'UNKNOWN'                       --email_top_level_domain_nm
    ,'UNKNOWN'                       --email_domain_nm
    ,'UNKNOWN'                       --email_valid_status_cd
    ,'UNKNOWN'                       --email_valid_typ
    ,CURRENT_DATETIME                --integrate_insert_datetime
    ,{{ dag_run.id }}                --integrate_insert_batch_number
    ,CURRENT_DATETIME                --integrate_update_datetime
    ,{{ dag_run.id }}                --integrate_update_batch_number
);


--DIM_NAME
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_name` WHERE nm_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_name` (
    nm_adw_key
    ,raw_first_nm
    ,raw_middle_nm
    ,raw_last_nm
    ,raw_suffix_nm
    ,raw_title_nm
    ,cleansed_first_nm
    ,cleansed_middle_nm
    ,cleansed_last_nm
    ,cleansed_suffix_nm
    ,cleansed_title_nm
    ,nm_valid_status_cd
    ,nm_valid_typ
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                              --nm_adw_key
    ,'UNKNOWN'                        --raw_first_nm
    ,'UNKNOWN'                        --raw_middle_nm
    ,'UNKNOWN'                        --raw_last_nm
    ,'UNKNOWN'                        --raw_suffix_nm
    ,'UNKNOWN'                        --raw_title_nm
    ,'UNKNOWN'                        --cleansed_first_nm
    ,'UNKNOWN'                        --cleansed_middle_nm
    ,'UNKNOWN'                        --cleansed_last_nm
    ,'UNKNOWN'                        --cleansed_suffix_nm
    ,'UNKNOWN'                        --cleansed_title_nm
    ,'UNKNOWN'                        --nm_valid_status_cd
    ,'UNKNOWN'                        --nm_valid_typ
    ,CURRENT_DATETIME                 --integrate_insert_datetime
    ,{{ dag_run.id }}                 --integrate_insert_batch_number
    ,CURRENT_DATETIME                 --integrate_update_datetime
    ,{{ dag_run.id }}                 --integrate_update_batch_number
);


--DIM_PHONE
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_phone` WHERE phone_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_phone` (
    phone_adw_key
    ,raw_phone_nbr
    ,cleansed_ph_nbr
    ,cleansed_ph_area_cd_nbr
    ,cleansed_ph_prefix_nbr
    ,cleansed_ph_suffix_nbr
    ,cleansed_ph_extension_nbr
    ,phone_valid_status_cd
    ,phone_valid_typ
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                             --phone_adw_key
    ,'UNKNOWN'                       --raw_phone_nbr
    ,'UNKNOWN'                       --cleansed_ph_nbr
    ,-1                              --cleansed_ph_area_cd_nbr
    ,-1                              --cleansed_ph_prefix_nbr
    ,-1                              --cleansed_ph_suffix_nbr
    ,-1                              --cleansed_ph_extension_nbr
    ,'UNKNOWN'                       --phone_valid_status_cd
    ,'UNKNOWN'                       --phone_valid_typ
    ,CURRENT_DATETIME                --integrate_insert_datetime
    ,{{ dag_run.id }}                --integrate_insert_batch_number
    ,CURRENT_DATETIME                --integrate_update_datetime
    ,{{ dag_run.id }}                --integrate_update_batch_number
);

--DIM_EMPLOYEE
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee` WHERE emp_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee` (
    emp_adw_key
    ,active_directory_email_adw_key
    ,email_adw_key
    ,phone_adw_key
    ,fax_phone_adw_key
    ,nm_adw_key
    ,emp_typ_cd
    ,emp_active_directory_user_id 
    ,emp_hr_id 
    ,emp_hr_hire_dt
    ,emp_hr_term_dt
    ,emp_hr_term_reason
    ,emp_hr_status
    ,emp_hr_region
    ,emp_loc_hr
    ,emp_hr_title
    ,emp_hr_sprvsr
    ,emp_hr_job_cd
    ,emp_hr_cost_center
    ,emp_hr_position_typ
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'              --emp_adw_key
    ,'-1'             --active_directory_email_adw_key
    ,'-1'             --email_adw_key
    ,'-1'             --phone_adw_key
    ,'-1'             --fax_phone_adw_key
    ,'-1'             --nm_adw_key
    ,'UNKNOWN'        --emp_typ_cd
    ,'UNKNOWN'        --emp_active_directory_user_id 
    ,'UNKNOWN'        --emp_hr_id 
    ,'1900-01-01'     --emp_hr_hire_dt
    ,'1900-01-01'     --emp_hr_term_dt
    ,'UNKNOWN'        --emp_hr_term_reason
    ,'UNKNOWN'        --emp_hr_status
    ,'UNKNOWN'        --emp_hr_region
    ,'UNKNOWN'        --emp_loc_hr
    ,'UNKNOWN'        --emp_hr_title
    ,'UNKNOWN'        --emp_hr_sprvsr
    ,'UNKNOWN'        --emp_hr_job_cd
    ,'UNKNOWN'        --emp_hr_cost_center
    ,'UNKNOWN'        --emp_hr_position_typ
    ,'1900-01-01'     --effective_start_datetime
    ,'9999-12-31'     --effective_end_datetime
    ,'N'              --actv_ind
    ,'-1'             --adw_row_hash
    ,CURRENT_DATETIME --integrate_insert_datetime
    ,{{ dag_run.id }} --integrate_insert_batch_number
    ,CURRENT_DATETIME --integrate_update_datetime
    ,{{ dag_run.id }} --integrate_update_batch_number
);

--DIM_ACA_OFFICE
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office` WHERE aca_office_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office` (
    aca_office_adw_key
    ,aca_office_address
    ,aca_office_state_cd
    ,aca_office_phone_nbr
    ,aca_office_list_source_key
    ,aca_office_nm
    ,aca_office_status
    ,aca_office_typ
    ,mbrs_branch_cd
    ,ins_department_cd
    ,rs_asstc_communicator_center
    ,car_care_loc_nbr
    ,tvl_branch_cd
    ,aca_office_division
    ,retail_sales_office_cd
    ,retail_manager
    ,retail_mngr_email
    ,retail_assistant_manager
    ,retail_assistant_mngr_email
    ,staff_assistant
    ,staff_assistant_email
    ,car_care_manager
    ,car_care_mngr_email
    ,car_care_assistant_manager
    ,car_care_assistant_mngr_email
    ,region_tvl_sales_mgr
    ,region_tvl_sales_mgr_email
    ,retail_sales_coach
    ,retail_sales_coach_email
    ,xactly_retail_store_nbr
    ,xactly_car_care_store_nbr
    ,region
    ,car_care_general_manager
    ,car_care_general_mngr_email
    ,car_care_dirctr
    ,car_care_dirctr_email
    ,car_care_district_nbr
    ,car_care_cost_center_nbr
    ,tire_cost_center_nbr
    ,admin_cost_center_nbr
    ,occupancy_cost_center_nbr
    ,retail_general_manager
    ,retail_general_mngr_email
    ,district_manager
    ,district_mngr_email
    ,retail_district_nbr
    ,retail_cost_center_nbr
    ,tvl_cost_center_nbr
    ,mbrs_cost_center_nbr
    ,facility_cost_center_nbr
    ,national_ats_nbr
    ,mbrs_division_ky
    ,mbrs_division_nm
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
     '-1'                   --aca_office_adw_key
    ,'UNKNOWN'              --aca_office_address
    ,'UNKNOWN'              --aca_office_state_cd
    ,'UNKNOWN'              --aca_office_phone_nbr
    ,'-3'                   --aca_office_list_source_key
    ,'UNKNOWN'              --aca_office_nm
    ,'UNKNOWN'              --aca_office_status
    ,'UNKNOWN'              --aca_office_typ
    ,'UNKNOWN'              --mbrs_branch_cd
    ,'UNKNOWN'              --ins_department_cd
    ,'UNKNOWN'              --rs_asstc_communicator_center
    ,'UNKNOWN'              --car_care_loc_nbr
    ,'UNKNOWN'              --tvl_branch_cd
    ,'UNKNOWN'              --aca_office_division
    ,'UNKNOWN'              --retail_sales_office_cd
    ,'UNKNOWN'              --retail_manager
    ,'UNKNOWN'              --retail_mngr_email
    ,'UNKNOWN'              --retail_assistant_manager
    ,'UNKNOWN'              --retail_assistant_mngr_email
    ,'UNKNOWN'              --staff_assistant
    ,'UNKNOWN'              --staff_assistant_email
    ,'UNKNOWN'              --car_care_manager
    ,'UNKNOWN'              --car_care_mngr_email
    ,'UNKNOWN'              --car_care_assistant_manager
    ,'UNKNOWN'              --car_care_assistant_mngr_email
    ,'UNKNOWN'              --region_tvl_sales_mgr
    ,'UNKNOWN'              --region_tvl_sales_mgr_email
    ,'UNKNOWN'              --retail_sales_coach
    ,'UNKNOWN'              --retail_sales_coach_email
    ,'UNKNOWN'              --xactly_retail_store_nbr
    ,'UNKNOWN'              --xactly_car_care_store_nbr
    ,'UNKNOWN'              --region
    ,'UNKNOWN'              --car_care_general_manager
    ,'UNKNOWN'              --car_care_general_mngr_email
    ,'UNKNOWN'              --car_care_dirctr
    ,'UNKNOWN'              --car_care_dirctr_email
    ,'UNKNOWN'              --car_care_district_nbr
    ,'UNKNOWN'              --car_care_cost_center_nbr
    ,'UNKNOWN'              --tire_cost_center_nbr
    ,'UNKNOWN'              --admin_cost_center_nbr
    ,'UNKNOWN'              --occupancy_cost_center_nbr
    ,'UNKNOWN'              --retail_general_manager
    ,'UNKNOWN'              --retail_general_mngr_email
    ,'UNKNOWN'              --district_manager
    ,'UNKNOWN'              --district_mngr_email
    ,'UNKNOWN'              --retail_district_nbr
    ,'UNKNOWN'              --retail_cost_center_nbr
    ,'UNKNOWN'              --tvl_cost_center_nbr
    ,'UNKNOWN'              --mbrs_cost_center_nbr
    ,'UNKNOWN'              --facility_cost_center_nbr
    ,'UNKNOWN'              --national_ats_nbr
    ,'UNKNOWN'				--mbrs_division_ky
    ,'UNKNOWN'				--mbrs_division_nm
    ,'1900-01-01'           --effective_start_datetime
    ,'9999-12-31'           --effective_end_datetime
    ,'N'                    --actv_ind
    ,'-1'                   --adw_row_hash
    , CURRENT_DATETIME      --integrate_insert_datetime
    , {{ dag_run.id }}      --integrate_insert_batch_number
    , CURRENT_DATETIME      --integrate_update_datetime
    , {{ dag_run.id }}      --integrate_update_batch_number
);


--DIM_BUSINESS_LINE
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_business_line` WHERE biz_line_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_business_line` (
    biz_line_adw_key
    ,biz_line_cd
    ,biz_line_desc
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
)
VALUES
(
    '-1'                --biz_line_adw_key
    ,'UNKNOWN'          --biz_line_cd
    ,'UNKNOWN'          --biz_line_desc
    ,CURRENT_DATETIME   --integrate_insert_datetime
    , {{ dag_run.id }}  --integrate_insert_batch_number)
);


--DIM_COMMENT
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment` WHERE comment_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment` (
    comment_adw_key
    ,created_emp_adw_key
    ,resolved_emp_adw_key
    ,comment_relation_adw_key
    ,comment_source_system_nm
    ,comment_source_system_key
    ,comment_creation_dtm
    ,comment_text
    ,comment_resolved_dtm
    ,comment_resltn_text
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'               --comment_adw_key
    ,'-1'              --created_emp_adw_key
    ,'-1'              --resolved_emp_adw_key
    ,'-1'              --comment_relation_adw_key
    ,'UNKNOWN'         --comment_source_system_nm
    ,-1                --comment_source_system_key
    ,'1900-01-01'      --comment_creation_dtm
    ,'UNKNOWN'         --comment_text
    ,'1900-01-01'      --comment_resolved_dtm
    ,'UNKNOWN'         --comment_resltn_text
    ,'1900-01-01'      --effective_start_datetime
    ,'9999-12-31'      --effective_end_datetime
    ,'N'               --actv_ind
    ,'-1'              --adw_row_hash
    ,CURRENT_DATETIME  --integrate_insert_datetime
    ,{{ dag_run.id }}  --integrate_insert_batch_number
    ,CURRENT_DATETIME  --integrate_update_datetime
    ,{{ dag_run.id }}  --integrate_update_batch_number
);


--DIM_EMPLOYEE_ROLE
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role` WHERE emp_role_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role` (
    emp_role_adw_key
    ,emp_adw_key
    ,nm_adw_key
    ,aca_office_adw_key
    ,emp_biz_role_line_cd
    ,emp_role_id
    ,emp_role_typ
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'               --emp_role_adw_key
    ,'-1'              --emp_adw_key
    ,'-1'              --nm_adw_key
    ,'-1'              --aca_office_adw_key
    ,'UNKNOWN'         --emp_biz_role_line_cd
    ,'UNKNOWN'         --emp_role_id
    ,'UNKNOWN'         --emp_role_typ
    ,'1900-01-01'      --effective_start_datetime
    ,'9999-12-31'      --effective_end_datetime
    ,'N'               --actv_ind
    ,'-1'              --adw_row_hash
    ,CURRENT_DATETIME  --integrate_insert_datetime
    ,{{ dag_run.id }}  --integrate_insert_batch_number
    ,CURRENT_DATETIME  --integrate_update_datetime
    ,{{ dag_run.id }}  --integrate_update_batch_number
);


--DIM_PRODUCT
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product` WHERE product_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product` (
    product_adw_key
    ,product_category_adw_key
    ,vendor_adw_key
    ,product_sku
    ,product_sku_key
    ,product_sku_desc
    ,product_sku_effective_dt
    ,product_sku_expiration_dt
    ,product_item_nbr_cd
    ,product_item_desc
    ,product_vendor_nm
    ,product_service_category_cd
    ,product_status_cd
    ,product_dynamic_sku
    ,product_dynamic_sku_desc
    ,product_dynamic_sku_key
    ,product_unit_cost
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                     --product_adw_key
    ,'-1'                    --product_category_adw_key
    ,'-1'                    --vendor_adw_key
    ,'UNKNOWN'               --product_sku
    ,'-1'                    --product_sku_key
    ,'UNKNOWN'               --product_sku_desc
    ,'1900-01-01'            --product_sku_effective_dt
    ,'1900-01-01'            --product_sku_expiration_dt
    ,'UNKNOWN'               --product_item_nbr_cd
    ,'UNKNOWN'               --product_item_desc
    ,'UNKNOWN'               --product_vendor_nm
    ,'UNKNOWN'               --product_service_category_cd
    ,'UNKNOWN'               --product_status_cd
    ,'UNKNOWN'               --product_dynamic_sku
    ,'UNKNOWN'               --product_dynamic_sku_desc
    ,'-1'                    --product_dynamic_sku_key
    ,0                       --product_unit_cost
    ,'1900-01-01'            --effective_start_datetime
    ,'9999-12-31'            --effective_end_datetime
    ,'UNKNOWN'               --actv_ind
    ,'-1'                    --adw_row_hash
    ,CURRENT_DATETIME        --integrate_insert_datetime
    ,{{ dag_run.id }}        --integrate_insert_batch_number
    ,CURRENT_DATETIME        --integrate_update_datetime
    ,{{ dag_run.id }}        --integrate_update_batch_number
);


--DIM_PRODUCT_CATEGORY
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product_category` WHERE product_category_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product_category` (
     product_category_adw_key
    ,biz_line_adw_key
    ,product_category_cd
    ,product_category_desc
    ,product_category_status_cd
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                      --product_category_adw_key
    ,'-1'                     --biz_line_adw_key
    ,'UNKNOWN'                --product_category_cd
    ,'UNKNOWN'                --product_category_desc
    ,'UNKNOWN'                --product_category_status_cd
    ,'1900-01-01'             --effective_start_datetime
    ,'9999-12-31'             --effective_end_datetime
    ,'N'                      --actv_ind
    ,'-1'                     --adw_row_hash
    ,CURRENT_DATETIME         --integrate_insert_datetime
    ,{{ dag_run.id }}         --integrate_insert_batch_number
    ,CURRENT_DATETIME         --integrate_update_datetime
    ,{{ dag_run.id }}         --integrate_update_batch_number
);


--DIM_VENDOR
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor` WHERE vendor_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor` (
    vendor_adw_key
    ,biz_line_adw_key
    ,address_adw_key
    ,email_adw_key
    ,vendor_cd
    ,vendor_name
    ,vendor_typ
    ,internal_facility_cd
    ,vendor_contact
    ,preferred_vendor
    ,vendor_status
    ,service_region
    ,service_region_desc
    ,fleet_ind
    ,aaa_facility
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                       --vendor_adw_key
    ,'-1'                      --biz_line_adw_key
    ,'-1'                      --address_adw_key
    ,'-1'                      --email_adw_key
    ,'UNKNOWN'                 --vendor_cd
    ,'UNKNOWN'                 --vendor_name
    ,'UNKNOWN'                 --vendor_typ
    ,-1                        --internal_facility_cd
    ,'UNKNOWN'                 --vendor_contact
    ,'UNKNOWN'                 --preferred_vendor
    ,'UNKNOWN'                 --vendor_status
    ,'UNKNOWN'                 --service_region
    ,'UNKNOWN'                 --service_region_desc
    ,'U'                       --fleet_ind
    ,'UNKNOWN'                 --aaa_facility
    ,'1900-01-01'              --effective_start_datetime
    ,'9999-12-31'              --effective_end_datetime
    ,'N'                       --actv_ind
    ,'-1'                      --adw_row_hash
    ,CURRENT_DATETIME          --integrate_insert_datetime
    ,{{ dag_run.id }}          --integrate_insert_batch_number
    ,CURRENT_DATETIME          --integrate_update_datetime
    ,{{ dag_run.id }}          --integrate_update_batch_number
);


--DIM_MEMBER
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member` WHERE mbr_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member` (
    mbr_adw_key
    ,mbrs_adw_key
    ,contact_adw_key
    ,mbrs_solicit_adw_key
    ,emp_role_adw_key
    ,mbr_source_system_key
    ,mbrs_id 
    ,mbr_assoc_id 
    ,mbr_ck_dgt_nbr
    ,mbr_status_cd
    ,mbr_extended_status_cd
    ,mbr_expiration_dt
    ,mbr_card_expiration_dt
    ,prim_mbr_cd
    ,mbr_do_not_renew_ind
    ,mbr_billing_cd
    ,mbr_billing_category_cd
    ,mbr_status_dtm
    ,mbr_cancel_dt
    ,mbr_join_aaa_dt
    ,mbr_join_club_dt
    ,mbr_reason_joined_cd
    ,mbr_assoc_relation_cd
    ,mbr_solicit_cd
    ,mbr_source_of_sale_cd
    ,mbr_comm_cd
    ,mbr_future_cancel_ind
    ,mbr_future_cancel_dt
    ,mbr_renew_method_cd
    ,free_mbr_ind
    ,previous_club_mbrs_id 
    ,previous_club_cd
    ,mbr_merged_previous_key
    ,mbr_merged_previous_club_cd
    ,mbr_mbrs_merged_previous_id 
    ,mbr_assoc_merged_previous_id
    ,mbr_merged_previous_ck_dgt_nbr
    ,mbr_merged_previous_mbr16_id 
    ,mbr_change_nm_dtm
    ,mbr_email_changed_dtm
    ,mbr_bad_email_ind
    --,mbr_email_optout_ind
    ,mbr_web_last_login_dtm
    ,mbr_active_expiration_dt
    ,mbr_actvtn_dt
    --,mbr_dn_text_ind
    ,mbr_duplicate_email_ind
    --,mbr_dn_call_ind
    --,mbr_no_email_ind
    --,mbr_dn_mail_ind
    ,mbr_dn_ask_for_email_ind
    --,mbr_dn_email_ind
    ,mbr_refused_give_email_ind
    ,mbrs_purge_ind
    ,payment_plan_future_cancel_dt
    ,payment_plan_future_cancel_ind
    ,mbr_card_typ_cd
    ,mbr_card_typ_desc
    ,entitlement_start_dt
    ,entitlement_end_dt
    ,mbr_previous_expiration_dt
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                   --mbr_adw_key
    ,'-1'                  --mbrs_adw_key
    ,'-1'                  --contact_adw_key
    ,'-1'                  --mbrs_solicit_adw_key
    ,'-1'                  --emp_role_adw_key
    ,-1                    --mbr_source_system_key
    ,'UNKNOWN'             --mbrs_id 
    ,'UNKNOWN'             --mbr_assoc_id 
    ,-1                    --mbr_ck_dgt_nbr
    ,'UNKNOWN'             --mbr_status_cd
    ,'UNKNOWN'             --mbr_extended_status_cd
    ,'1900-01-01'          --mbr_expiration_dt
    ,'1900-01-01'          --mbr_card_expiration_dt
    ,'U'             --prim_mbr_cd
    ,'U'             --mbr_do_not_renew_ind
    ,'UNKNOWN'             --mbr_billing_cd
    ,'UNKNOWN'             --mbr_billing_category_cd
    ,'1900-01-01'          --mbr_status_dtm
    ,'1900-01-01'          --mbr_cancel_dt
    ,'1900-01-01'          --mbr_join_aaa_dt
    ,'1900-01-01'          --mbr_join_club_dt
    ,'UNKNOWN'             --mbr_reason_joined_cd
    ,'UNKNOWN'             --mbr_assoc_relation_cd
    ,'UNKNOWN'             --mbr_solicit_cd
    ,'UNKNOWN'             --mbr_source_of_sale_cd
    ,'UNKNOWN'             --mbr_comm_cd
    ,'U'             --mbr_future_cancel_ind
    ,'1900-01-01'          --mbr_future_cancel_dt
    ,'UNKNOWN'             --mbr_renew_method_cd
    ,'U'             --free_mbr_ind
    ,'UNKNOWN'             --previous_club_mbrs_id 
    ,'UNKNOWN'             --previous_club_cd
    ,-1                    --mbr_merged_previous_key
    ,'UNKNOWN'             --mbr_merged_previous_club_cd
    ,'UNKNOWN'             --mbr_mbrs_merged_previous_id 
    ,'UNKNOWN'             --mbr_assoc_merged_previous_id
    ,'UNKNOWN'             --mbr_merged_previous_ck_dgt_nbr
    ,'UNKNOWN'             --mbr_merged_previous_mbr16_id 
    ,'1900-01-01'          --mbr_change_nm_dtm
    ,'1900-01-01'          --mbr_email_changed_dtm
    ,'U'             --mbr_bad_email_ind
    --,'UNKNOWN'             --mbr_email_optout_ind
    ,'1900-01-01'          --mbr_web_last_login_dtm
    ,'1900-01-01'          --mbr_active_expiration_dt
    ,'1900-01-01'          --mbr_actvtn_dt
    --,'UNKNOWN'             --mbr_dn_text_ind
    ,'U'             --mbr_duplicate_email_ind
    --,'UNKNOWN'             --mbr_dn_call_ind
    --,'UNKNOWN'             --mbr_no_email_ind
    --,'UNKNOWN'             --mbr_dn_mail_ind
    ,'U'             --mbr_dn_ask_for_email_ind
    --,'UNKNOWN'             --mbr_dn_email_ind
    ,'U'             --mbr_refused_give_email_ind
    ,'U'             --mbrs_purge_ind
    ,'1900-01-01'          --payment_plan_future_cancel_dt
    ,'UNKNOWN'             --payment_plan_future_cancel_ind
    ,'UNKNOWN'             --mbr_card_typ_cd
    ,'UNKNOWN'             --mbr_card_typ_desc
    ,'1900-01-01'          --entitlement_start_dt
    ,'1900-01-01'          --entitlement_end_dt
    ,'1900-01-01'          --mbr_previous_expiration_dt
    ,'1900-01-01'          --effective_start_datetime
    ,'9999-12-31'          --effective_end_datetime
    ,'N'                   --actv_ind
    ,'-1'                  --adw_row_hash
    ,CURRENT_DATETIME      --integrate_insert_datetime
    ,{{ dag_run.id }}      --integrate_insert_batch_number
    ,CURRENT_DATETIME      --ntegrate_update_datetime
    ,{{ dag_run.id }}      --integrate_update_batch_number
);

--DIM_MEMBER_AUTO_RENEWAL_CARD
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_auto_renewal_card` WHERE mbr_ar_card_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_auto_renewal_card` (
     mbr_ar_card_adw_key
    ,nm_adw_key
    ,address_adw_key
    ,mbr_source_arc_key
    ,mbr_arc_status_dtm
    ,mbr_cc_typ_cd
    ,mbr_cc_expiration_dt
    ,mbr_cc_reject_reason_cd
    ,mbr_cc_reject_dt
    ,mbr_cc_donor_nbr
    ,mbr_cc_last_four
    ,mbr_credit_debit_typ_cd
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                   --mbr_ar_card_adw_key
    ,'-1'                  --nm_adw_key
    ,'-1'                  --address_adw_key
    ,-1                    --mbr_source_arc_key
    ,'1900-01-01'          --mbr_arc_status_dtm
    ,'UNKNOWN'             --mbr_cc_typ_cd
    ,'1900-01-01'          --mbr_cc_expiration_dt
    ,'UNKNOWN'             --mbr_cc_reject_reason_cd
    ,'1900-01-01'          --mbr_cc_reject_dt
    ,'UNKNOWN'             --mbr_cc_donor_nbr
    ,'UNKNOWN'             --mbr_cc_last_four
    ,'UNKNOWN'             --mbr_credit_debit_typ_cd
    ,'1900-01-01'          --effective_start_datetime
    ,'9999-12-31'          --effective_end_datetime
    ,'N'                   --actv_ind
    ,'-1'                  --adw_row_hash
    ,CURRENT_DATETIME      --integrate_insert_datetime
    ,{{ dag_run.id }}      --integrate_insert_batch_number
    ,CURRENT_DATETIME      --integrate_update_datetime
    ,{{ dag_run.id }}      --integrate_update_batch_number
);



--DIM_MEMBER_RIDER
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider` WHERE mbr_rider_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider` (
    mbr_rider_adw_key
    ,mbrs_adw_key
    ,mbr_adw_key
    ,mbr_ar_card_adw_key
    ,emp_role_adw_key
    ,product_adw_key
    ,mbr_rider_source_key
    ,mbr_rider_status_cd
    ,mbr_rider_status_dtm
    ,mbr_rider_cancel_dt
    ,mbr_rider_cost_effective_dt
    ,mbr_rider_solicit_cd
    ,mbr_rider_do_not_renew_ind
    ,mbr_rider_dues_cost_amt
    ,mbr_rider_dues_adj_amt
    ,mbr_rider_payment_amt
    ,mbr_rider_paid_by_cd
    ,mbr_rider_future_cancel_dt
    ,mbr_rider_billing_category_cd
    ,mbr_rider_cancel_reason_cd
    ,mbr_rider_reinstate_ind
    ,mbr_rider_effective_dt
    ,mbr_rider_original_cost_amt
    ,mbr_rider_reinstate_reason_cd
    ,mbr_rider_extend_exp_amt
    ,mbr_rider_actual_cancel_dtm
    ,mbr_rider_def_key
    ,mbr_rider_actvtn_dt
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                            --mbr_rider_adw_key
    ,'-1'                            --mbrs_adw_key
    ,'-1'                            --mbr_adw_key
    ,'-1'                            --mbr_ar_card_adw_key
    ,'-1'                            --emp_role_adw_key
    ,'-1'                            --product_adw_key
    ,-1                              --mbr_rider_source_key
    ,'UNKNOWN'                       --mbr_rider_status_cd
    ,'1900-01-01'                    --mbr_rider_status_dtm
    ,'1900-01-01'                    --mbr_rider_cancel_dt
    ,'1900-01-01'                    --mbr_rider_cost_effective_dt
    ,'UNKNOWN'                       --mbr_rider_solicit_cd
    ,'U'                       --mbr_rider_do_not_renew_ind
    ,0                               --mbr_rider_dues_cost_amt
    ,0                               --mbr_rider_dues_adj_amt
    ,0                               --mbr_rider_payment_amt
    ,'UNKNOWN'                       --mbr_rider_paid_by_cd
    ,'1900-01-01'                    --mbr_rider_future_cancel_dt
    ,'UNKNOWN'                       --mbr_rider_billing_category_cd
    ,'UNKNOWN'                       --mbr_rider_cancel_reason_cd
    ,'U'                             --mbr_rider_reinstate_ind
    ,'1900-01-01'                    --mbr_rider_effective_dt
    ,0                               --mbr_rider_original_cost_amt
    ,'UNKNOWN'                       --mbr_rider_reinstate_reason_cd
    ,0                               --mbr_rider_extend_exp_amt
    ,'1900-01-01'                    --mbr_rider_actual_cancel_dtm
    ,-1                              --mbr_rider_def_key
    ,'1900-01-01'                    --mbr_rider_actvtn_dt
    ,'1900-01-01'                    --effective_start_datetime
    ,'9999-12-31'                    --effective_end_datetime
    ,'N'                             --actv_ind
    ,'-1'                            --adw_row_hash
    ,CURRENT_DATETIME                --integrate_insert_datetime
    ,{{ dag_run.id }}                --integrate_insert_batch_number
    ,CURRENT_DATETIME                --integrate_update_datetime
    ,{{ dag_run.id }}                --integrate_update_batch_number
);


--DIM_MEMBERSHIP
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership` WHERE mbrs_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership` (
    mbrs_adw_key
    ,address_adw_key
    ,phone_adw_key
    ,product_adw_key
    ,aca_office_adw_key
    ,mbrs_source_system_key
    ,mbrs_id 
    ,mbrs_status_cd
    ,mbrs_billing_cd
    ,mbrs_status_dtm
    ,mbrs_cancel_dt
    ,mbrs_billing_category_cd
    ,mbrs_dues_cost_amt
    ,mbrs_dues_adj_amt
    ,mbrs_future_cancel_ind
    ,mbrs_future_cancel_dt
    ,mbrs_out_of_territory_cd
    ,mbrs_address_change_dtm
    ,mbrs_cdx_export_update_dtm
    ,mbrs_tier_key
    ,mbrs_temporary_address_ind
    ,previous_club_mbrs_id 
    ,previous_club_cd
    ,mbrs_transfer_club_cd
    ,mbrs_transfer_dtm
    ,mbrs_merged_previous_club_cd
    ,mbrs_mbrs_merged_previous_id 
    ,mbrs_mbrs_merged_previous_key
    ,mbrs_dn_solicit_ind
    ,mbrs_bad_address_ind
    ,mbrs_dn_send_pubs_ind
    ,mbrs_market_tracking_cd
    ,mbrs_salvage_ind
    ,mbrs_salvaged_ind
    ,mbrs_dn_salvage_ind
    ,mbrs_group_cd
    ,mbrs_typ_cd
    ,mbrs_ebill_ind
    ,mbrs_marketing_segmntn_cd
    ,mbrs_promo_cd
    ,mbrs_phone_typ_cd
    ,mbrs_ers_abuser_ind
    --,mbrs_dn_send_aaawld_ind
    ,mbrs_shadow_mcycle_ind
    ,mbrs_student_ind
    ,mbrs_address_ncoa_update_ind
    ,mbrs_assigned_retention_ind
    ,mbrs_mbrs_no_promotion_ind
    --,mbrs_dn_telemkt_ind
    ,mbrs_dn_offer_plus_ind
    ,mbrs_aaawld_online_ed_ind
    ,mbrs_shadowrv_coverage_ind
    --,mbrs_dn_direct_mail_slc_ind
    ,mbrs_heavy_ers_ind
    ,mbrs_purge_ind
    ,mbrs_unapplied_amt
    ,mbrs_advance_payment_amt
    ,mbrs_balance_amt
    ,payment_plan_future_cancel_dt
    ,payment_plan_future_cancel_ind
    ,switch_prim_ind
    ,supress_bill_ind
    ,offer_end_dt
    ,old_payment_plan_ind
    ,carry_over_amt
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                            --mbrs_adw_key
    ,'-1'                            --address_adw_key
    ,'-1'                            --phone_adw_key
    ,'-1'                            --product_adw_key
    ,'-1'                            --aca_office_adw_key
    ,-1                              --mbrs_source_system_key
    ,'UNKNOWN'                       --mbrs_id 
    ,'UNKNOWN'                       --mbrs_status_cd
    ,'UNKNOWN'                       --mbrs_billing_cd
    ,'1900-01-01'                    --mbrs_status_dtm
    ,'1900-01-01'                    --mbrs_cancel_dt
    ,'UNKNOWN'                       --mbrs_billing_category_cd
    ,0                               --mbrs_dues_cost_amt
    ,0                               --mbrs_dues_adj_amt
    ,'U'                       --mbrs_future_cancel_ind
    ,'1900-01-01'                    --mbrs_future_cancel_dt
    ,'UNKNOWN'                       --mbrs_out_of_territory_cd
    ,'1900-01-01'                    --mbrs_address_change_dtm
    ,'1900-01-01'                    --mbrs_cdx_export_update_dtm
    ,-1                              --mbrs_tier_key
    ,'U'                       --mbrs_temporary_address_ind
    ,'UNKNOWN'                       --previous_club_mbrs_id 
    ,'UNKNOWN'                       --previous_club_cd
    ,'UNKNOWN'                       --mbrs_transfer_club_cd
    ,'1900-01-01'                    --mbrs_transfer_dtm
    ,'UNKNOWN'                       --mbrs_merged_previous_club_cd
    ,'UNKNOWN'                       --mbrs_mbrs_merged_previous_id 
    ,-1                              --mbrs_mbrs_merged_previous_key
    ,'U'                       --mbrs_dn_solicit_ind
    ,'U'                       --mbrs_bad_address_ind
    ,'U'                       --mbrs_dn_send_pubs_ind
    ,'UNKNOWN'                       --mbrs_market_tracking_cd
    ,'U'                       --mbrs_salvage_ind
    ,'U'                       --mbrs_salvaged_ind
    ,'U'                       --mbrs_dn_salvage_ind
    ,'UNKNOWN'                       --mbrs_group_cd
    ,'UNKNOWN'                       --mbrs_typ_cd
    ,'U'                       --mbrs_ebill_ind
    ,'UNKNOWN'                       --mbrs_marketing_segmntn_cd
    ,'UNKNOWN'                       --mbrs_promo_cd
    ,'UNKNOWN'                       --mbrs_phone_typ_cd
    ,'U'                       --mbrs_ers_abuser_ind
    --,'UNKNOWN'                       --mbrs_dn_send_aaawld_ind
    ,'U'                       --mbrs_shadow_mcycle_ind
    ,'U'                       --mbrs_student_ind
    ,'U'                       --mbrs_address_ncoa_update_ind
    ,'U'                       --mbrs_assigned_retention_ind
    ,'U'                       --mbrs_mbrs_no_promotion_ind
    --,'UNKNOWN'                       --mbrs_dn_telemkt_ind
    ,'U'                       --mbrs_dn_offer_plus_ind
    ,'U'                       --mbrs_aaawld_online_ed_ind
    ,'U'                       --mbrs_shadowrv_coverage_ind
    --,'UNKNOWN'                       --mbrs_dn_direct_mail_slc_ind
    ,'U'                       --mbrs_heavy_ers_ind
    ,'U'                       --mbrs_purge_ind
    ,0                               --mbrs_unapplied_amt
    ,0                               --mbrs_advance_payment_amt
    ,0                               --mbrs_balance_amt
    ,'1900-01-01'                    --payment_plan_future_cancel_dt
    ,'UNKNOWN'                       --payment_plan_future_cancel_ind
    ,'UNKNOWN'                       --switch_prim_ind
    ,'UNKNOWN'                       --supress_bill_ind
    ,'1900-01-01'                    --offer_end_dt
    ,'UNKNOWN'                       --old_payment_plan_ind
    ,0                               --carry_over_amt
    ,'1900-01-01'                    --effective_start_datetime
    ,'9999-12-31'                    --effective_end_datetime
    ,'N'                             --actv_ind
    ,'-1'                            --adw_row_hash
    ,CURRENT_DATETIME                --integrate_insert_datetime
    ,{{ dag_run.id }}                --integrate_insert_batch_number
    ,CURRENT_DATETIME                --integrate_update_datetime
    ,{{ dag_run.id }}                --integrate_update_batch_number
);


--DIM_MEMBERSHIP_FEE
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee` WHERE mbrs_fee_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee` (
    mbrs_fee_adw_key
    ,mbr_adw_key
    ,product_adw_key
    ,mbrs_fee_source_key
    ,status
    ,fee_typ
    ,fee_dt
    ,waived_dt
    ,waived_by
    ,donor_nbr
    ,waived_reason_cd
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                      --mbrs_fee_adw_key
    ,'-1'                      --mbr_adw_key
    ,'-1'                      --product_adw_key
    ,-1                        --mbrs_fee_source_key
    ,'UNKNOWN'                 --status
    ,'UNKNOWN'                 --fee_typ
    ,'1900-01-01'              --fee_dt
    ,'1900-01-01'              --waived_dt
    ,-1                        --waived_by
    ,'UNKNOWN'                 --donor_nbr
    ,'UNKNOWN'                 --waived_reason_cd
    ,'1900-01-01'              --effective_start_datetime
    ,'9999-12-31'              --effective_end_datetime
    ,'N'                       --actv_ind
    ,'-1'                      --adw_row_hash
    ,CURRENT_DATETIME          --integrate_insert_datetime
    ,{{ dag_run.id }}          --integrate_insert_batch_number
    ,CURRENT_DATETIME          --integrate_update_datetime
    ,{{ dag_run.id }}          --integrate_update_batch_number
);



--DIM_MEMBERSHIP_MARKETING_SEGMENTATION
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_mbrs_marketing_segmntn` WHERE mbrs_marketing_segmntn_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_mbrs_marketing_segmntn` (
    mbrs_marketing_segmntn_adw_key
    ,mbrs_adw_key
    ,mbr_expiration_dt
    ,segmntn_test_group_cd
    ,segmntn_panel_cd
    ,segmntn_control_panel_ind
    ,segmntn_comm_cd
    ,segment_nm
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                        --mbrs_marketing_segmntn_adw_key
    ,'-1'                        --mbrs_adw_key
    ,'1900-01-01'                --mbr_expiration_dt
    ,'U'                   --segmntn_test_group_cd
    ,'UNKNOWN'                   --segmntn_panel_cd
    ,'U'                   --segmntn_control_panel_ind
    ,'UNKNOWN'                   --segmntn_comm_cd
    ,'UNKNOWN'                   --segment_nm
    ,'1900-01-01'                --effective_start_datetime
    ,'9999-12-31'                --effective_end_datetime
    ,'N'                         --actv_ind
    ,'-1'                        --adw_row_hash
    ,CURRENT_DATETIME            --integrate_insert_datetime
    ,{{ dag_run.id }}            --integrate_insert_batch_number
    ,CURRENT_DATETIME            --integrate_update_datetime
    ,{{ dag_run.id }}            --integrate_update_batch_number
);


--DIM_MEMBERSHIP_PAYMENT_PLAN
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_payment_plan` WHERE mbrs_payment_plan_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_payment_plan` (
    mbrs_payment_plan_adw_key
    ,mbrs_adw_key
    ,mbr_rider_adw_key
    ,payment_plan_source_key
    ,payment_plan_nm
    ,payment_plan_payment_cnt
    ,payment_plan_month
    ,payment_nbr
    ,payment_plan_status
    ,payment_plan_charge_dt
    ,safety_fund_donation_ind
    ,payment_amt
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                             --mbrs_payment_plan_adw_key
    ,'-1'                            --mbrs_adw_key
    ,'-1'                            --mbr_rider_adw_key
    ,-1                              --payment_plan_source_key
    ,'UNKNOWN'                       --payment_plan_nm
    ,0                               --payment_plan_payment_cnt
    ,0                               --payment_plan_month
    ,0                               --payment_nbr
    ,'UNKNOWN'                       --payment_plan_status
    ,'1900-01-01'                    --payment_plan_charge_dt
    ,'U'                       --safety_fund_donation_ind
    ,0                               --payment_amt
    ,'1900-01-01'                    --effective_start_datetime
    ,'9999-12-31'                    --effective_end_datetime
    ,'N'                             --actv_ind
    ,'-1'                            --adw_row_hash
    ,CURRENT_DATETIME                --integrate_insert_datetime
    ,{{ dag_run.id }}                --integrate_insert_batch_number
    ,CURRENT_DATETIME                --integrate_update_datetime
    ,{{ dag_run.id }}                --integrate_update_batch_number
);


--DIM_MEMBERSHIP_SOLICITATION
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation` WHERE mbrs_solicit_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation` (
    mbrs_solicit_adw_key
    ,mbrs_solicit_source_system_key
    ,solicit_cd
    ,solicit_group_cd
    ,mbr_typ_cd
    ,solicit_camp_cd
    ,solicit_category_cd
    ,solicit_discount_cd
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                          --mbrs_solicit_adw_key
    ,-1                           --mbrs_solicit_source_system_key
    ,'UNKNOWN'                    --solicit_cd
    ,'UNKNOWN'                    --solicit_group_cd
    ,'UNKNOWN'                    --mbr_typ_cd
    ,'UNKNOWN'                    --solicit_camp_cd
    ,'UNKNOWN'                    --solicit_category_cd
    ,'UNKNOWN'                    --solicit_discount_cd
    ,'1900-01-01'                 --effective_start_datetime
    ,'9999-12-31'                 --effective_end_datetime
    ,'N'                          --actv_ind
    ,'-1'                         --adw_row_hash
    ,CURRENT_DATETIME             --integrate_insert_datetime
    ,{{ dag_run.id }}             --integrate_insert_batch_number
    ,CURRENT_DATETIME             --integrate_update_datetime
    ,{{ dag_run.id }}             --integrate_update_batch_number
);

--MEMBERSHIP_BILLING_SUMMARY
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.membership_billing_summary` WHERE mbrs_billing_summary_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.membership_billing_summary` (
    mbrs_billing_summary_adw_key	
    ,mbrs_adw_key	
    ,bill_summary_source_key
    ,bill_process_dt	
    ,bill_notice_nbr
    ,bill_amt	
    ,bill_credit_amt	
    ,bill_typ
    ,mbr_expiration_dt	
    ,bill_paid_amt	
    ,bill_renewal_method
    ,bill_panel_cd	
    ,bill_ebilling_ind	
    ,effective_start_datetime	
    ,effective_end_datetime	
    ,actv_ind	
    ,adw_row_hash	
    ,integrate_insert_datetime	
    ,integrate_insert_batch_number	
    ,integrate_update_datetime	
    ,integrate_update_batch_number	
)
VALUES
(
    '-1'                          --mbrs_billing_summary_adw_key
    ,'-1'                         --mbrs_adw_key
    ,-1                           --bill_summary_source_key
    ,'1900-01-01'                 --bill_process_dt
    ,-1                           --bill_notice_nbr
    ,0                            --bill_amt
    ,0                            --bill_credit_amt
    ,'UNKNOWN'                    --bill_typ
	,'1900-01-01'                 --mbr_expiration_dt	
    ,0                            --bill_paid_amt	
    ,'UNKNOWN'                    --bill_renewal_method	
    ,'UNKNOWN'                    --bill_panel_cd	
    ,'U'                          --bill_ebilling_ind
    ,'1900-01-01'                 --effective_start_datetime
    ,'9999-12-31'                 --effective_end_datetime
    ,'N'                          --actv_ind
    ,'-1'                         --adw_row_hash
    ,CURRENT_DATETIME             --integrate_insert_datetime
    ,{{ dag_run.id }}             --integrate_insert_batch_number
    ,CURRENT_DATETIME             --integrate_update_datetime
    ,{{ dag_run.id }}             --integrate_update_batch_number
);

--MBRS_PAYMENT_APPLIED_SUMMARY 
DELETE FROM `{{var.value.INTEGRATION_PROJECT}}.adw.mbrs_payment_applied_summary` WHERE mbrs_payment_apd_summ_adw_key = '-1';
INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.mbrs_payment_applied_summary` (
    mbrs_payment_apd_summ_adw_key
    ,mbrs_payment_apd_rev_adw_key
    ,mbrs_adw_key
    ,mbrs_payment_received_adw_key
    ,aca_office_adw_key
    ,emp_adw_key
    ,payment_applied_source_key
    ,payment_applied_dt
    ,payment_applied_amt
    ,payment_batch_nm
    ,payment_method_cd
    ,payment_method_desc
    ,payment_typ_cd
    ,payment_typ_desc
    ,payment_adj_cd
    ,payment_adj_desc
    ,payment_created_dtm
    ,payment_paid_by_cd
    ,payment_unapplied_amt
    ,advance_payment_amt
    ,payment_source_cd
    ,payment_source_desc
    ,payment_tran_typ_cd
    ,payment_tran_typ_cdesc
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                          --mbrs_payment_apd_summ_adw_key
    ,'-1'                         --mbrs_payment_apd_rev_adw_key
   ,'-1'                         --mbrs_adw_key
   ,'-1'                         --mbrs_payment_received_adw_key
   ,'-1'                         --aca_office_adw_key
   ,'-1'                         --emp_adw_key
    ,-1                           --payment_applied_source_key
    ,'1900-01-01'                 --payment_applied_dt
    ,0                            --payment_applied_amt
    ,'UNKNOWN'                    --payment_batch_nm
    ,'UNKNOWN'                    --payment_method_cd  STRING NULLABLE
    ,'UNKNOWN'                    --payment_method_desc    STRING NULLABLE
    ,'UNKNOWN'                    --payment_typ_cd STRING NULLABLE
    ,'UNKNOWN'                    --payment_typ_desc   STRING NULLABLE
    ,'UNKNOWN'                    --payment_adj_cd STRING NULLABLE
    ,'UNKNOWN'                    --payment_adj_desc   STRING NULLABLE
    ,'1900-01-01'                 --payment_created_dtm    DATETIME   NULLABLE
    ,'UNKNOWN'                    --payment_paid_by_cd STRING NULLABLE
    ,0                            --payment_unapplied_amt  NUMERIC    NULLABLE
    ,0                            --advance_payment_amt
    ,'UNKNOWN'                    --payment_source_cd  STRING NULLABLE
    ,'UNKNOWN'                    --payment_source_desc    STRING NULLABLE
    ,'UNKNOWN'                    --payment_tran_typ_cd    STRING NULLABLE
    ,'UNKNOWN'                    --payment_tran_typ_cdesc     STRING NULLABLE
    ,'1900-01-01'                 --effective_start_datetime
    ,'9999-12-31'                 --effective_end_datetime
    ,'N'                          --actv_ind
    ,'-1'                         --adw_row_hash
    ,CURRENT_DATETIME             --integrate_insert_datetime
    ,{{ dag_run.id }}             --integrate_insert_batch_number
    ,CURRENT_DATETIME             --integrate_update_datetime
    ,{{ dag_run.id }}             --integrate_update_batch_number
);

--  MEMBERSHIP_PAYMENT_RECEIVED
DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received` WHERE mbrs_payment_received_adw_key = '-1';
INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`
(
     mbrs_payment_received_adw_key
    ,mbrs_adw_key
    ,mbrs_billing_summary_adw_key
    ,mbr_ar_card_adw_key
    ,aca_office_adw_key
    ,emp_adw_key
    ,payment_received_source_key
    ,payment_received_batch_key
    ,payment_received_nm
    ,payment_received_cnt
    ,payment_received_expected_amt
    ,payment_received_created_dtm
    ,payment_received_status
    ,payment_received_status_dtm
    ,payment_received_amt
    ,payment_received_method_cd
    ,payment_received_method_desc
    ,payment_source_cd
    ,payment_source_desc
    ,payment_typ_cd
    ,payment_typ_desc
    ,payment_reason_cd
    ,payment_paid_by_desc
    ,payment_adj_cd
    ,payment_adj_desc
    ,payment_received_post_ind
    ,payment_received_post_dtm
    ,payment_received_ar_ind
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
Values
(
     '-1'                                   -- mbrs_payment_received_adw_key
    ,'-1'                                   -- mbrs_adw_key
    ,'-1'                                   -- mbrs_billing_summary_adw_key
    ,'-1'                                   -- mbr_ar_card_adw_key
    ,'-1'                                   -- aca_office_adw_key
    ,'-1'                                   -- emp_adw_key
    ,SAFE_CAST('-1' AS INT64)               -- payment_received_source_key,
    ,SAFE_CAST('-1' AS INT64)               -- payment_received_batch_key,
    ,'UNKNOWN'                              -- payment_received_nm
    ,SAFE_CAST('-1' AS INT64)               -- payment_received_cnt
    ,SAFE_CAST('-1' AS NUMERIC)             -- payment_received_expected_amt
    ,SAFE_CAST('1900-01-01' AS DATETIME)    -- payment_received_created_dtm
    ,'UNKNOWN'                              -- payment_received_status
    ,SAFE_CAST('1900-01-01' AS DATETIME)    -- payment_received_status_dtm,
    ,SAFE_CAST('-1' AS NUMERIC)             -- payment_received_amt,
    ,'UNKNOWN'                              -- payment_received_method_cd,
    ,'UNKNOWN'                              -- payment_received_method_desc,
    ,'UNKNOWN'                              -- payment_source_cd,
    ,'UNKNOWN'                              -- payment_source_desc,
    ,'UNKNOWN'                              -- payment_typ_cd,
    ,'UNKNOWN'                              -- payment_typ_desc,
    ,'UNKNOWN'                              -- payment_reason_cd,
    ,'UNKNOWN'                              -- payment_paid_by_desc,
    ,'UNKNOWN'                              -- payment_adj_cd,
    ,'UNKNOWN'                              -- payment_adj_desc,
    ,'U'                              -- payment_received_post_ind,
    ,SAFE_CAST('1900-01-01' AS DATETIME)    -- payment_received_post_dtm,
    ,'U'                              -- payment_received_ar_ind,
    ,SAFE_CAST('1900-01-01' AS DATETIME)    -- effective_start_datetime,
    ,SAFE_CAST('9999-12-31' AS DATETIME)    -- effective_end_datetime,
    ,'Y'                                    -- actv_ind
    ,'-1'                                   -- adw_row_hash
    ,CURRENT_DATETIME                       -- integrate_insert_datetime
    ,{{ dag_run.id }}                       -- integrate_insert_batch_number
    ,CURRENT_DATETIME                       -- integrate_update_datetime
    ,{{ dag_run.id }}                       -- integrate_update_batch_number
);

--admin.ingestion_batch
DELETE FROM `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_batch` WHERE batch_number = -1;
INSERT INTO `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_batch` (
    batch_number
    , batch_status
    , batch_start_datetime
    , batch_end_datetime
)
VALUES
(
    -1                --batch_number
    ,'active'           --batch_status
    ,safe_cast(CURRENT_DATETIME as String)   --batch_start_datetime
    ,safe_cast(CURRENT_DATETIME as String)   --batch_end_datetime)
);


DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail` WHERE mbrs_payment_apd_dtl_adw_key = '-1';
INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail` ( 
	mbrs_payment_apd_dtl_adw_key,	
	mbr_adw_key,	
	mbrs_payment_apd_summ_adw_key,	
	product_adw_key,	
	payment_apd_dtl_source_key,	
	payment_applied_dt,	
	payment_method_cd,	
	payment_method_desc,	
	payment_detail_amt,
	payment_applied_unapplied_amt,	
	discount_amt,	
	pymt_apd_discount_counted,	
	discount_effective_dt,	
	rider_cost_effective_dtm,	
	effective_start_datetime,	
	effective_end_datetime,	
	actv_ind,	
	adw_row_hash,	
	integrate_insert_datetime,	
	integrate_insert_batch_number,	
	integrate_update_datetime,	
	integrate_update_batch_number	 )
VALUES
  (  	
'-1' 				--mbrs_payment_apd_dtl_adw_key
,'-1'				--mbr_adw_key
,'-1'				--mbrs_payment_apd_summ_adw_key
,'-1'   			--product_adw_key
,-1 				--payment_apd_dtl_source_key
,'1900-01-01'		--payment_applied_dt
,'UNKNOWN'			--payment_method_cd
,'UNKNOWN'			--payment_method_desc
,0 					--payment_detail_amt
,0 					--payment_applied_unapplied_amt
,0 					--discount_amt
,'UNKNOWN'			--pymt_apd_discount_counted
,'1900-01-01'		--discount_effective_dt
,'1900-01-01'		--rider_cost_effective_dtm
,'1900-01-01'		--effective_start_datetime
,'9999-12-31' 		--effective_end_datetime
,'N' 				--actv_ind
,'-1'				--adw_row_hash
,CURRENT_DATETIME	--integrate_insert_datetime
,{{ dag_run.id }} 	--integrate_insert_batch_number
,CURRENT_DATETIME	--integrate_update_datetime
,{{ dag_run.id }} 	--integrate_update_batch_number	
    );


--Insurance_policy
DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` WHERE ins_policy_adw_key = '-1';
--Insurance_policy
INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` (
    ins_policy_adw_key,
    channel_adw_key,
    biz_line_adw_key,
    product_category_adw_key,
    ins_quote_adw_key,
    state_adw_key,
    emp_adw_key,
    ins_policy_system_source_key,
    ins_policy_quote_ind,
    ins_policy_number,
    ins_policy_effective_dt,
    ins_policy_Expiration_dt,
    ins_policy_cntrctd_exp_dt,
    ins_policy_annualized_comm,
    ins_policy_annualized_premium,
    ins_policy_billed_comm,
    ins_policy_billed_premium,
    ins_policy_estimated_comm,
    ins_policy_estimated_premium,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
VALUES
(
'-1'                  --  ins_policy_adw_key,
,'-1'                  --  channel_adw_key,
,'-1'                  --  biz_line_adw_key,
,'-1'                  --  product_category_adw_key,
,'-1'                  --  ins_quote_adw_key,
,'-1'                  --  state_adw_key,
,'-1'                  --  emp_adw_key,
,SAFE_CAST('-2' AS INT64)--  ins_policy_system_source_key,
,'U'             --  ins_policy_quote_ind,
,'UNKNOWN'             --  ins_policy_number,
,'1900-01-01'          --  ins_policy_effective_dt,
,'1900-01-01'          --  ins_policy_Expiration_dt,
,'1900-01-01'          --  ins_policy_cntrctd_exp_dt,
,0                      --  ins_policy_annualized_comm,
,0                      --  ins_policy_annualized_premium,
,0                      --  ins_policy_billed_comm,
,0                      --  ins_policy_billed_premium,
,0                      --  ins_policy_estimated_comm,
,0                      --  ins_policy_estimated_premium,
,'1900-01-01'           --  effective_start_datetime,
,'9999-12-31'           --  effective_end_datetime,
,'UNKNOWN'              --  actv_ind,
,'-1'                    --  adw_row_hash,
, CURRENT_DATETIME       --  integrate_insert_datetime,
,{{ dag_run.id }}       --  integrate_insert_batch_number,
,CURRENT_DATETIME       --  integrate_update_datetime,
,{{ dag_run.id }}       --  integrate_update_batch_number
);

--adw.dim_state
DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_state` WHERE state_adw_key = '-1';
INSERT INTO  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_state` ( state_adw_key,
    state_cd,
    state_nm,
    country_cd,
    country_nm,
    region_nm,
    integrate_insert_datetime,
    integrate_insert_batch_number )
   values(
     '-1',  -- state_adw_key
    'UN',-- state_cd
    'UNKNOWN',-- state_nm
    'UNK',-- country_cd
    'UNKNOWN',-- country_nm
    'UNKNOWN',-- region_nm
    CURRENT_DATETIME ,-- integrate_insert_datetime
    {{ dag_run.id }} -- integrate_insert_batch_number
  );
--adw.dim_contact_segmentation
DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation` WHERE contact_segmntn_adw_key = '-1';
INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`
(
    contact_segmntn_adw_key
    ,contact_adw_key
    ,segment_source
    ,segment_panel
    ,segment_cd
    ,segment_Desc
    ,actual_score_ind
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrated_insert_datetime
    ,integrated_insert_batch_number
    ,integrated_update_datetime
    ,integrated_update_batch_number
)
Values
(
    '-1'                    --contact_segmntn_adw_key
   ,'-1'                    --contact_adw_key
   ,'unknown'               --segment_source
   ,'unknown'               --segment_panel
   ,'unknown'               --segment_cd
   ,'unknown'               --segment_Desc
   ,'N'                     --actual_score_ind
    ,'1900-01-01'           --effective_start_datetime
    ,'9999-12-31'           --effective_end_datetime
    ,'0'                    --actv_ind
    ,'-1'                   --adw_row_hash
    ,CURRENT_DATETIME       --integrate_insert_dtm
    ,{{ dag_run.id }}       --integrate_insert_batch_nbr
    ,CURRENT_DATETIME       --integrate_update_dtm
    ,{{ dag_run.id }}       --integrate_update_batch_nbr
);