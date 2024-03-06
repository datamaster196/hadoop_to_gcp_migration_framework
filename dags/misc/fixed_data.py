#######################################################################################################################
##  DAG_NAME :  UTIL-FIXED-DATA-INSERTION
##  PURPOSE :   DAG to populate stubbed records for table and popultae data steward
##  PREREQUISITE DAGs :
#######################################################################################################################

from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "UTIL-FIXED-DATA-INSERTION"

dim_product_category_sql = "INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category`(" \
                           "product_category_adw_key," \
                           "biz_line_adw_key," \
                           "product_category_cd," \
                           "product_category_desc," \
                           "product_category_status_cd," \
                           "effective_start_datetime," \
                           "effective_end_datetime," \
                           "actv_ind," \
                           "adw_row_hash," \
                           "integrate_insert_datetime," \
                           "integrate_insert_batch_number," \
                           "integrate_update_datetime," \
                           "integrate_update_batch_number) " \
                           "VALUES (" \
                           "'574ad6f5-1233-42ff-bb2b-ebda5e204c31', '33b05324-9a68-4775-9c89-eee81867b23d', 'MBR_FEES' , 'MEMBERSHIP FEES'       ,'' , '1900-01-01', '2099-12-31', 'Y',''  ,CURRENT_DATETIME , -1 , CURRENT_DATETIME , -1)," \
                           "('48124364-3e3e-4194-8880-8ddcdfb5acc9', '33b05324-9a68-4775-9c89-eee81867b23d', 'RCC'      , 'RIDER COMPONENT CODES' ,'' , '1900-01-01', '2099-12-31', 'Y', '' ,CURRENT_DATETIME , -1 , CURRENT_DATETIME , -1) , " \
                           "('eefce1e6-3fee-4ba2-a19c-0e54b29abc72', '33b05324-9a68-4775-9c89-eee81867b23d', 'BA'       , 'BILL ADJUSTMENT'       ,'' , '1900-01-01', '2099-12-31', 'Y', '' ,CURRENT_DATETIME , -1 , CURRENT_DATETIME , -1) "


dim_business_line = "INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_business_line` (" \
                    "biz_line_adw_key," \
                    "biz_line_cd," \
                    "biz_line_desc," \
                    "integrate_insert_datetime," \
                    "integrate_insert_batch_number) " \
                    "VALUES " \
                    "('33b05324-9a68-4775-9c89-eee81867b23d' , '3' ,  'Member Services' , CURRENT_DATETIME  , -1 )," \
                    "('8b5e22eb-570a-45db-ae20-6fb30e3e5204', 'ATO', 'Automotive Solutions', CURRENT_DATETIME, -1)," \
                    "('292584af-57f8-4d8c-912f-7c12df811fc1', 'CC', 'Car Care', CURRENT_DATETIME, -1), " \
                    "('328c05c2-8b5b-4570-92a7-07338add8474', 'CL', 'Commercial Insurance', CURRENT_DATETIME, -1), " \
                    "('ed7da038-43b0-443a-92fa-e035941b1ba6', 'CT', 'Corporate Travel', CURRENT_DATETIME, -1), " \
                    "('7969632b-0e74-420a-a605-85bcd68e957a', 'CTC', 'Contact Centers', CURRENT_DATETIME, -1), " \
                    "('c467d752-96d1-45a0-9f80-2ce877dbf25d', 'Discount', 'Discounts', CURRENT_DATETIME, -1), " \
                    "('c0aace40-626b-4c8e-bffe-97155baafd92', 'DRV', 'Driver Services', CURRENT_DATETIME, -1), " \
                    "('52913c1a-bd2c-4fe8-bb83-7a37c19017fb', 'FIN', 'Finance', CURRENT_DATETIME, -1), " \
                    "('1627c4ca-300d-45b1-bdb5-72c91dd2ec33', 'FND', 'Traffic Safety', CURRENT_DATETIME, -1), " \
                    "('87b3a51b-9344-423f-a78f-86dc92f489b3', 'Found', 'Foundation', CURRENT_DATETIME, -1), " \
                    "('125715f1-9ded-4683-a2bb-77d82f1a53fc', 'FS', 'Financial Services', CURRENT_DATETIME, -1), " \
                    "('0e6cfdcd-ebbf-475c-a93a-1e1c405a3996', 'FSD', 'Financial Services & Discounts', CURRENT_DATETIME, -1), " \
                    "('90be2f81-b0a5-4e14-bd15-7c7761c5ca7c', 'HR', 'Human Resources', CURRENT_DATETIME, -1), " \
                    "('49e503c9-8f79-4574-8391-68bfc6ca8f5d', 'INS', 'Insurance', CURRENT_DATETIME, -1), " \
                    "('aa24f481-84b0-4b1d-b847-7719198d054b', 'IT', 'Information Technology', CURRENT_DATETIME, -1), " \
                    "('1ee7aba2-9f5f-432d-8d62-f69f15e0b5c5', 'LGL', 'Legal', CURRENT_DATETIME, -1), " \
                    "('400f9b55-affc-4daf-9f1c-e2304f5b1525', 'LT', 'Leisure Travel', CURRENT_DATETIME, -1), " \
                    "('33b05324-9a68-4775-9c89-eee81867b23d', 'MBR', 'Membership', CURRENT_DATETIME, -1), " \
                    "('f80097f9-d56a-4451-a0a0-0a2c421a97f7', 'MKT', 'Marketing', CURRENT_DATETIME, -1), " \
                    "('56aa2b0d-c4d5-4d88-90ea-dfeb62c5398a', 'OPR', 'Operations Support Admin', CURRENT_DATETIME, -1), " \
                    "('c3940fa2-8d43-4027-aa6c-71ffe000823a', 'PGA', 'Public Relations & Government Affairs', CURRENT_DATETIME, -1), " \
                    "('01035672-d6ee-4ffe-86c0-51aae2447294', 'PL', 'Personal Lines Insurance', CURRENT_DATETIME, -1), " \
                    "('87e0ce35-34c5-4d19-8339-830daf5ba6af', 'RTL', 'Retail Operations', CURRENT_DATETIME, -1)," \
                    "('869f187f-5222-4a41-86a2-c56c1eccbf1f', 'TRV', 'Travel', CURRENT_DATETIME, -1)," \
                    "('07b2d2b4-99b2-4f6a-8a0c-f861364bf013', 'WRL', 'RAAA World', CURRENT_DATETIME, -1) "




dim_vendor = "INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor` (" \
             "vendor_adw_key," \
             "biz_line_adw_key," \
             "address_adw_key," \
             "email_adw_key," \
             "vendor_cd," \
             "vendor_name," \
             "vendor_typ," \
             "internal_facility_cd," \
             "vendor_contact," \
             "preferred_vendor," \
             "vendor_status," \
             "service_region," \
             "service_region_desc," \
             "fleet_ind," \
             "aaa_facility," \
             "effective_start_datetime," \
             "effective_end_datetime," \
             "actv_ind," \
             "adw_row_hash," \
             "integrate_insert_datetime," \
             "integrate_insert_batch_number," \
             "integrate_update_datetime," \
             "integrate_update_batch_number) " \
             "VALUES ('6e3dfbd4-2ce0-41a1-ade9-6407d1c2fe52','33b05324-9a68-4775-9c89-eee81867b23d','-1','-1','ACA','AAA Club Alliance','Membership',null,'','','Active','','','U','','1900-01-01','2099-12-31','Y','',CURRENT_DATETIME,-1,CURRENT_DATETIME,-1)"

dim_product = "INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` (" \
              "product_adw_key," \
              "product_category_adw_key," \
              "vendor_adw_key," \
              "product_sku," \
              "product_sku_key," \
              "product_sku_desc," \
              "product_sku_effective_dt," \
              "product_sku_expiration_dt," \
              "product_item_nbr_cd," \
              "product_item_desc," \
              "product_vendor_nm," \
              "product_service_category_cd," \
              "product_status_cd," \
              "product_dynamic_sku," \
              "product_dynamic_sku_desc," \
              "product_dynamic_sku_key," \
              "product_unit_cost," \
              "effective_start_datetime," \
              "effective_end_datetime," \
              "actv_ind," \
              "adw_row_hash," \
              "integrate_insert_datetime," \
              "integrate_insert_batch_number," \
              "integrate_update_datetime," \
              "integrate_update_batch_number)" \
              "VALUES ('ec4d187f-ed53-4d12-890e-a9a848c5d42e' , 'eefce1e6-3fee-4ba2-a19c-0e54b29abc72'  ,'6e3dfbd4-2ce0-41a1-ade9-6407d1c2fe52', 'BA' , ' BA' ,' Bill Adjustment' ,NULL,NULL,'','','','','','','','',NULL, '1900-01-01', '2099-12-31', 'Y', 'pLuxrB5O19pcVI7DHH0SeA==' ,CURRENT_DATETIME , -1 , CURRENT_DATETIME , -1)"

dim_office = "INSERT INTO" \
             "  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office`" \
             "  (" \
             "      aca_office_adw_key," \
             "      aca_office_address," \
             "      aca_office_state_cd," \
             "      aca_office_phone_nbr," \
             "      aca_office_list_source_key," \
             "      aca_office_nm, " \
             "      aca_office_status," \
             "      aca_office_typ," \
             "      mbrs_branch_cd," \
             "      ins_department_cd," \
             "      rs_asstc_communicator_center," \
             "      car_care_loc_nbr," \
             "      tvl_branch_cd," \
             "      aca_office_division," \
             "      retail_sales_office_cd," \
             "      retail_manager," \
             "      retail_mngr_email," \
             "      retail_assistant_manager," \
             "      retail_assistant_mngr_email," \
             "      staff_assistant," \
             "      staff_assistant_email," \
             "      car_care_manager," \
             "      car_care_mngr_email," \
             "      car_care_assistant_manager," \
             "      car_care_assistant_mngr_email," \
             "      region_tvl_sales_mgr," \
             "      region_tvl_sales_mgr_email," \
             "      retail_sales_coach," \
             "      retail_sales_coach_email," \
             "      xactly_retail_store_nbr," \
             "      xactly_car_care_store_nbr," \
             "      region," \
             "      car_care_general_manager," \
             "      car_care_general_mngr_email," \
             "      car_care_dirctr," \
             "      car_care_dirctr_email," \
             "      car_care_district_nbr," \
             "      car_care_cost_center_nbr," \
             "      tire_cost_center_nbr," \
             "      admin_cost_center_nbr," \
             "      occupancy_cost_center_nbr," \
             "      retail_general_manager, " \
             "      retail_general_mngr_email," \
             "      district_manager," \
             "      district_mngr_email," \
             "      retail_district_nbr," \
             "      retail_cost_center_nbr," \
             "      tvl_cost_center_nbr," \
             "      mbrs_cost_center_nbr," \
             "      facility_cost_center_nbr," \
             "      national_ats_nbr," \
             "      mbrs_division_ky,"\
             "      mbrs_division_nm,"\
             "      effective_start_datetime," \
             "      effective_end_datetime," \
             "      actv_ind," \
             "      adw_row_hash," \
             "      integrate_insert_datetime," \
             "      integrate_insert_batch_number," \
             "      integrate_update_datetime," \
             "      integrate_update_batch_number" \
             "  ) " \
             "SELECT" \
             "  '-2' AS aca_office_adw_key," \
             "  NULL AS aca_office_address," \
             "  NULL AS aca_office_state_cd," \
             "  NULL AS aca_office_phone_nbr," \
             "  '-2' AS aca_office_list_source_key," \
             "  'Enterprise Level Office' aca_office_nm," \
             "  NULL AS aca_office_status," \
             "  NULL AS aca_office_typ," \
             "  NULL AS mbrs_branch_cd," \
             "  NULL AS ins_department_cd," \
             "  NULL AS rs_asstc_communicator_center," \
             "  NULL AS car_care_loc_nbr," \
             "  NULL AS tvl_branch_cd," \
             "  NULL AS aca_office_division," \
             "  NULL AS retail_sales_office_cd," \
             "  NULL AS retail_manager," \
             "  NULL AS retail_mngr_email," \
             "  NULL AS retail_assistant_manager," \
             "  NULL AS retail_assistant_mngr_email," \
             "  NULL AS staff_assistant," \
             "  NULL AS staff_assistant_email," \
             "  NULL AS car_care_manager," \
             "  NULL AS car_care_mngr_email," \
             "  NULL AS car_care_assistant_manager," \
             "  NULL AS car_care_assistant_mngr_email," \
             "  NULL AS region_tvl_sales_mgr," \
             "  NULL AS region_tvl_sales_mgr_email," \
             "  NULL AS retail_sales_coach," \
             "  NULL AS retail_sales_coach_email," \
             "  NULL AS xactly_retail_store_nbr," \
             "  NULL AS xactly_car_care_store_nbr," \
             "  NULL AS region," \
             "  NULL AS car_care_general_manager," \
             "  NULL AS car_care_general_mngr_email," \
             "  NULL AS car_care_dirctr," \
             "  NULL AS car_care_dirctr_email," \
             "  NULL AS car_care_district_nbr," \
             "  NULL AS car_care_cost_center_nbr," \
             "  NULL AS tire_cost_center_nbr," \
             "  NULL AS admin_cost_center_nbr," \
             "  NULL AS occupancy_cost_center_nbr," \
             "  NULL AS retail_general_manager," \
             "  NULL AS retail_general_mngr_email," \
             "  NULL AS district_manager," \
             "  NULL AS district_mngr_email," \
             "  NULL AS retail_district_nbr," \
             "  NULL AS retail_cost_center_nbr," \
             "  NULL AS tvl_cost_center_nbr," \
             "  NULL AS mbrs_cost_center_nbr," \
             "  NULL AS facility_cost_center_nbr," \
             "  NULL AS national_ats_nbr," \
             "  NULL AS mbrs_division_ky," \
             "  NULL AS mbrs_division_nm," \
             "  CAST('1900-01-01' as DATETIME) AS effective_start_datetime," \
             "  CAST('2099-12-31' as DATETIME) AS effective_end_datetime," \
             "  'Y' AS actv_ind," \
             "  '' AS adw_row_hash," \
             "  CURRENT_DATETIME AS integrate_insert_datetime," \
             "  -1 AS integrate_insert_batch_number," \
             "  CURRENT_DATETIME AS integrate_update_datetime," \
             "  -1 AS integrate_update_batch_number" \
             " FROM (" \
             "  SELECT" \
             "    1" \
             ") a " \
             "WHERE  0 = (  " \
             "SELECT" \
             "    COUNT(aca_office_adw_key)" \
             "  FROM " \
             "    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office`" \
             "  WHERE aca_office_list_source_key = '-2'" \
             "    );"
             
data_steward = "INSERT INTO "\
                " `{{var.value.INGESTION_PROJECT}}.admin.data_steward`"\
                "VALUES "\
                "( 'adw','audit_contact_match','contact_adw_key','(PK)(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','source_system_key_1','(PK)  Identifying key from source system, or first part of compound key.',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','source_system_key_2','(PK)  Secound part of compund key from source system if it exists',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','source_system_key_3','(PK)  Third part of compound key from source system if it exists',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','source_system_nm','(PK)  Name of Source System that this record came from',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','src_mch_on_mbr_id_ind','  Indicator to show if this matched to a record coming from the same source in the same load on the member id',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','src_mch_on_address_nm_ind','  Indicator to show if this matched to a record coming from the same source in the same load on the name and address using the city and state',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','src_mch_on_email_nm_ind','  Indicator to show if this matched to a record coming from the same source in the same load on the name and email',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','src_mch_on_zip_nm_ind','  Indicator to show if this matched to a record coming from the same source in the same load on the name and address using the zip',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','tgt_mch_on_mbr_id_ind','  Indicator to show if this matched to a record already in contact_info on the member id',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','tgt_mch_on_address_nm_ind','  Indicator to show if this matched to a record already in contact_info on the name and address using the city and state',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','tgt_mch_on_email_nm_ind','  Indicator to show if this matched to a record already in contact_info on the name and email address',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','tgt_mch_on_zip_nm_ind','  Indicator to show if this matched to a record already in contact_info on the name and address using the zip code',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','tgt_mch_on_source_key_ind','  Indicator to show if this matched to a record already in contact_info on the source system key',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','audit_contact_match','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_adw_key','(PK)  Unique Key for each ACA store record',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_address','  The physical address of the office.',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_division','  The division subset in addition to the region where the office is located',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_list_source_key','  The source key to the office Master Data repository.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_nm','  The conformed office name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_phone_nbr','  The primary phone number of the office.',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_state_cd','  The State Code in which the office is located.',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_status','  Indicate if the store is open of closed',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','aca_office_typ','  The type of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','admin_cost_center_nbr','  The admin Cost Center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_assistant_manager','  The Car Care Assistant Managers name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_assistant_mngr_email','  The Car Care Assistant Managers Email address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_cost_center_nbr','  The car care cost center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_dirctr','  The Car Care Directors name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_dirctr_email','  The Car Care Directors Email address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_district_nbr','  The car care district number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_general_manager','  The Car Care General Managers name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_general_mngr_email','  The Car Care General Managers Email address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_loc_nbr','  The numeric location identifier for the Car Care facility',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_manager','  The Car Care Managers name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','car_care_mngr_email','  The Car Care Managers Email address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','district_manager','  The District Managers name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','district_mngr_email','  The District  Managers email',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','facility_cost_center_nbr','  The facility cost center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','ins_department_cd','  The department code in Insurance',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','mbrs_branch_cd','  The Branch code used by Membership',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','mbrs_cost_center_nbr','  The membership cost center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','national_ats_nbr','  The national ATS  number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','occupancy_cost_center_nbr','  The Occupancy Cost Center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','region','  The ACA Region Name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','region_tvl_sales_mgr','  The Regional Travel Sales Managers name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','region_tvl_sales_mgr_email','  The Regional Travel Sales Managers Email address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_assistant_manager','  The Retail Assistant Managers name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_assistant_mngr_email','  The Retail Assistant Managers email address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_cost_center_nbr','  The retail cost center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_district_nbr','  The retail district number for the  Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_general_manager','  The Retail General Manager',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_general_mngr_email','  The Retail General Managers email',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_manager','  The Retail Manager for the Cost Center',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_mngr_email','  The Retail Manager Email Address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_sales_coach','  The Retail Sales Coachs name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_sales_coach_email','  The Retail Sales Coachs email',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','retail_sales_office_cd','  The location identifier for Retail Sales',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','rs_asstc_communicator_center','  The Communication Center location for Roadside Assistance',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','staff_assistant','  The Staff Assistants name',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','staff_assistant_email','  The Staff Assistants email address',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','tire_cost_center_nbr','  The tire Cost Center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','tvl_branch_cd','  The location identifier for the Travel Location',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','tvl_cost_center_nbr','  The travel cost center number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','xactly_car_care_store_nbr','  The xactly car care store number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','xactly_retail_store_nbr','  The xactly retail store number of the Office',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_aca_office','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','address_adw_key','(PK)  Unique key for each address record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','address_typ_cd','  Type of address that this record represents. This field may only be available after enhanced cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','address_valid_status_cd','  This field indicates if the cleansed address values tie out to a verified address',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','address_valid_typ','  This field describes the type of process that has been used to validate and/or enhance the address data',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_address_1_nm','  address 1 name after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_address_2_nm','  address 2 name after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_care_of_nm','  care of name after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_city_nm','  city name after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_country_nm','  Country name after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_postal_cd','  postal code after cleansing process',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_postal_plus_4_cd','  postal code plus 4 after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_state_cd','  State code after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','cleansed_state_nm','  full state name from cleansing process',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','county_fips','  County FIPS from the address cleansing process',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','county_nm','  County Name from source or from cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','latitude_nbr','  Latitude Number from source or from cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','longitude_nbr','  Longitude Number from source or from cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_address_1_nm','  raw address line 1 from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_address_2_nm','  raw address line 2 from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_care_of_nm','  raw care of name if included in address',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_city_nm','  raw full city name from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_country_nm','  raw abbreviation of country from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_postal_cd','  raw postal code from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_postal_plus_4_cd','  raw postal code plus 4 from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','raw_state_cd','  raw two letter abbreviation of the state from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','tm_zone_nm','  Time Zone of this address',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_address','integrate_update_batch_number','  Batch number of the most recent update to this record',CURRENT_DATETIME),"\
                "( 'adw','dim_business_line','biz_line_adw_key','(PK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','dim_business_line','biz_line_cd','  The code representing the Business Line.',CURRENT_DATETIME),"\
                "( 'adw','dim_business_line','biz_line_desc','  The description representing the Business Line.',CURRENT_DATETIME),"\
                "( 'adw','dim_business_line','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_business_line','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_club','club_adw_key','(PK)  Unique Key for each Policy Line record',CURRENT_DATETIME),"\
                "( 'adw','dim_club','effective_start_datetime','  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_club','address_adw_key','(FK)  Unique key for each address record.',CURRENT_DATETIME),"\
                "( 'adw','dim_club','club_cd','  The three digit code representing the AAA/CAA Club',CURRENT_DATETIME),"\
                "( 'adw','dim_club','club_iso_cd','  The ISO code 4th - 6th digit on the clubs member cards.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_club','club_merchant_nbr','  The merchant number under which the club is doing business.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_club','club_nm','  The name of the AAA/CAA club.',CURRENT_DATETIME),"\
                "( 'adw','dim_club','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_club','phone_adw_key','(FK)  Unique key for each phone record.',CURRENT_DATETIME),"\
                "( 'adw','dim_club','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_club','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_club','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_club','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_club','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_adw_key','(PK)  Unique key for each comment record',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_creation_dtm','  The date the comment was created',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_relation_adw_key','  Key that ties to the comment to the record the comment is about. The table this links to will be determined by the source system name. ',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_resltn_text','  The text of the resolution ',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_resolved_dtm','  The date a comment with an action item was resolved',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_source_system_key','  Membership Comment key from the MZP system',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_source_system_nm','  Business line that this comment is tied to',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','comment_text','  The user entered or system generated comment text',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','created_emp_adw_key','(FK)  Unique key for each ACA User record',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','resolved_emp_adw_key','(FK)  Unique key for each ACA User record',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_comment','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_adw_key','(PK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_address_1_nm','  Default first line of address as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_address_2_nm','  Default second line of address as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_birth_dt','  Date of birth of this contact info record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_care_of_nm','  Default care of name as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_city_nm','  Default city as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_country_nm','  Default country as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_email_nm','  Default email address as determined by the primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_first_nm','  Default first name as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_gender_cd','  Gender Code for this contact info record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_gender_nm','  Full name of gender for this contact info record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_last_nm','  Default last name as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_middle_nm','  Default middle name as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_phone_extension_nbr','  Extension portion of the phone number determined by the primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_phone_formatted_nbr','  Formatted default phone number as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_phone_nbr','  Default raw phone number as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_postal_cd','  Default postal code, specifically first 5 digits if US, as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_postal_plus_4_cd','  Default postal code plus four as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_state_cd','  Default 2 letter code for states as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_state_nm','  Default full state name as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_suffix_nm','  Default name suffix as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_title_nm','  Default title as determined by primacy rules',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','contact_typ_cd','  Code that indicates if contact is a person, organization, etc.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_contact_info','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','contact_adw_key','(PK)(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','source_1_key','  Either the source key or the first part of a composite key',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','source_2_key','  Second part of composite key',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','source_3_key','  Third part of composite key',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','source_4_key','  Fifth part of composite key',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','source_5_key','  Fifth part of composite key',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','contact_source_system_nm','(PK)  Name of the source system that this record was generated in',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','key_typ_nm','(PK)  Name of the type of key that will be stored',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_contact_source_key','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_ins_score_adw_key','(PK)  Unique key created for each Insurance Score record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_person_adw_key','(FK)  Unique key created for each Person record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_activity_tile','  Vertical: Auto Industry: Personal Lines Predictor: Shopping Propensity ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_channel_pref','  Indicates the consumers agent type preference:-independent, exclusive or direct. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_channel_pref_d_dcil','  Vertical: Auto Industry: Life and Personal Lines Predictor: Shopping Propensity Indicates the likelihood the consumer prefers a direct channel agent. Decile returned.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_channel_pref_e_dcil','  Vertical: Auto Industry: Life and Personal Lines Predictor: Shopping Propensity Indicates the likelihood the consumer prefers an exclusive channel agent. Decile returned.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_channel_pref_i_dcil','  Vertical: Auto  Demographic Channel Preference I Decile Industry: Life and Personal Lines Predictor: Shopping Propensity Indicates the likelihood the consumer prefers an independent channel agent.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_group_cd','  Score indicating likely persona. Score indicating likely persona.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_life_attrin_mdl_dcil','  Column: Life_Attrition_Model Industry: Life Prediction: Retention Improvement Life Attrition score used to prevent new life policies from high attrition rates. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_life_class_dcil','  Life Risk Decile',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_mkt_rsk_clsfir_auto_dcil','  Column: Marketing_Risk_Classifier_Auto_Decile Vertical: Auto Industry: Personal Lines Predictor: Risk Performance ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_pred_rnw_month_auto','  This is the month the Auto Insurance Policy is predicted to be renew.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_pred_rnw_month_home','  This is the month the Home Owners Insurance Policy is predicted to be renew.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_premium_dcil','  Model Number: MK25, MK26 Industry: Personal Lines Predictor: Financial/LTV Predictor ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','demo_prospect_survival_dcil','  Industry: Personal Lines Predictor: Retention Improvement Represents the likelihood that a prospect will remain a customer for at least a year or longer.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','integrate_snapshot_dt','  Datetime representing the monthly Market Magnifier Snapshot from which this record was loaded.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_ins_score','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','contact_adw_key','(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_age_group_cd','  Indicate the age group the person is in.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_birth_dt','  The birth date Lexus/Nexus has found for this person.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_biz_owner','  Indicates if the person is a Business Owner',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_dwelling_typ','  Each household is assigned a dwelling type code based on United States Postal Service (USPS) information. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_education_level','  Indicates if the persons education level.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_estimated_age','  The estimated age of the person from the Market Magnifier overlay.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_hsd_estimated_income','  Estimated Income is the total estimated income for a living unit calculated using a statistical model. The model predicts income using a variety of individual, household and geographic variables, including Census and Summarized Credit Statistics. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_ethnic_cntry_of_origin_cd','  Based on a comprehensive predictive name analysis process that identifies country of origin. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_ethnic_detail_cd','  Based on a comprehensive predictive name analysis process which identifies ethnicity. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_ethnic_grouping_cd','  Ethnicity Group Codes are rolled up versions of the Ethnicity Detailed Codes Based on a comprehensive predictive name analysis process that identifies ethnicity. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_ethnic_language_pref','  Based on a comprehensive predictive name analysis process that identifies language preference.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_ethnic_religion_cd','  Based on a comprehensive predictive name analysis process that identifies ethnicity. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_exact_age','  The exact age of the person from the Market Magnifier overlay.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_gender','  The gender Lexus/Nexus has found for this person.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_home_ownership_cd','  Indicate the probability of Home Ownership.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_income_range_cd','  Estimated Income is the total estimated income for a living unit calculated using a statistical model. The model predicts income using a variety of individual, household and geographic variables, including Census and Summarized Credit Statistics. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_length_of_residence','  Length of Residence (LOR) is the length of time the living unit has lived at the current address. The calculation is based on the first time the living unit was reported at that address. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_lexid','  The Lexus/Nexus identifier for the specified person.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_marital_status','  The marital status Lexus/Nexus has found for this person.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_occptn_cd','  Occupation codes are sourced from state licensing agencies. This information is further enhanced from directly reported survey. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_occptn_group','  Information is compiled from self-reported surveys, derived from state licensing agencies, or calculated through the application of predictive models. Position 1: K=Known, I=Inferred or U=Unknown Position 2: Occupation code 0-9 ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_person_adw_key','(PK)  Unique key created for each Person record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','demo_presence_of_children','  Indicates presence of a child in the household. ',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','integrate_snapshot_dt','  Datetime representing the monthly Market Magnifier Snapshot from which this record was loaded.',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_demo_person','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','email_adw_key','(PK)  Unique key for each email record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','cleansed_email_nm','  Cleansed email address used in matching',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','email_domain_nm','  Domain from the email address, for example gmail, yahoo, hotmail',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','email_top_level_domain_nm','  Top level domain in the email address, for example com or org',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','email_valid_status_cd','  This field indicates if the cleansed address values tie out to a verified email address',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','email_valid_typ','  This field describes the type of process that has been used to validate and/or enhance the email address data',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','raw_email_nm','  email address',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','integrate_update_datetime','  Datetime of the latest update to this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_email','integrate_update_batch_number','  Batch Number of the latest load that updated this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_adw_key','(PK)  Unique key for each ACA User record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','active_directory_email_adw_key','(FK)  Unique key for each email record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','email_adw_key','(FK)  Unique key for each email record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_active_directory_user_id','  The aaacorp user id',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_cost_center','  The HR Cost Center for the employee',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_hire_dt','  The date the employee has been hired',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_id','  The ID assigned to the employee.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_job_cd','  The HR Job code for the employee',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_loc_hr','  The location the employee has been assigned to by HR',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_position_typ','  Indicates if the employee is Full time or Part time',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_region','  The region the employee has been assigned to by HR',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_status','  The status of the employee',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_sprvsr','  The name of the employees supervisor',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_term_dt','  The date the employee has been terminated',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_term_reason','  The reason the employee was terminated',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_hr_title','  The employees title',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','emp_typ_cd','  E = Employee, S = System User, EA = Entrepreneurial Agent',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','fax_phone_adw_key','(FK)  Unique key for each phone record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','nm_adw_key','(FK)  Unique key for each name record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','phone_adw_key','(FK)  Unique key for each phone record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_employee','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','emp_adw_key','(FK)  Unique key for each ACA User record',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','effective_start_datetime','(PK)(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','aca_office_adw_key','(FK)  Store List ADW Key',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','emp_role_adw_key','(PK)  Unique key for each user role record',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','emp_biz_role_line_cd','  The code representing the Business Line this user represents',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','emp_role_id','  The natural key from the Membership Business Line identifying the Membership Sales Agent',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','emp_role_typ','  The type of the agent',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','nm_adw_key','(FK)  Unique key for each name record.',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_employee_role','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_adw_key','(PK)  Unique key for each member in the membership system',CURRENT_DATETIME),"\
                "( 'adw','dim_member','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_member','contact_adw_key','(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','dim_member','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_member','emp_role_adw_key','(FK)  Unique key for each user role record',CURRENT_DATETIME),"\
                "( 'adw','dim_member','free_mbr_ind','  Indicates the member is free',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_actvtn_dt','  The date the member became active',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_active_expiration_dt','  The expiration date to which the member_expiration_dt will be set when the member is paid in full.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_assoc_id','  The sequential associate member within the membership.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_assoc_relation_cd','  Indicates how the associate member is related to the primary.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_bad_email_ind','  Indicate the members email is bad',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_billing_category_cd','  The code which indicates the manner in which the riders for the member were priced.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_billing_cd','  The code indicating how the most recent billing was created.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_cancel_dt','  The date the member cancelled.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_card_expiration_dt','  The expiration date on the members card.  Since we issue multi year cards this will be set to a future date beyond the member_expriation_dt',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_ck_dgt_nbr','  The check (16th) digit on the membership card',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_comm_cd','  The most recent commission code indicating if the member is new, renewed or drop recovery.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_dn_ask_for_email_ind','  Do Not Ask for E-Mail',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_dn_call_ind','  Do Not Call',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_dn_email_ind','  Do Not Email',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_dn_mail_ind','  Do Not Mail',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_dn_text_ind','  Do Not Text',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_do_not_renew_ind','  Indicates to not renew this member.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_duplicate_email_ind','  Duplicate Email',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_email_changed_dtm','  The date the members email changed',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_email_optout_ind','  Indicate the member does not want to receive email',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_expiration_dt','  The date the member will expire.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_future_cancel_dt','  The future date a member will be cancelled when not renewing.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_future_cancel_ind','  Indicates if the membership will be cancelled in a future ',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_join_aaa_dt','  The date the member originally joined AAA/CAA.  For incoming transfers this will be a earlier date than the join_club_dt',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_join_club_dt','  The date the member joined ACA',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_change_nm_dtm','  The most recent date the members name was changed',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_no_email_ind','  No Email',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_reason_joined_cd','  The reason the member joined AAA.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_refused_give_email_ind','  Refused to Give Email',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_renew_method_cd','  Indicates if the member is Auto Renewed or will be billed.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_solicit_cd','  The solicitation (creative) which the member used to join AAA.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_source_of_sale_cd','  The method to which the member responded to join AAA.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_source_system_key','  The primary key in mz_member.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_status_cd','  The status of the member.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_status_dtm','  The date the member status changed.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_web_last_login_dtm','  The most recent date the member logged onto WebMember',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbrs_id','  7 digit Membership ID.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbrs_purge_ind','  Indicates if this member was purged',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbrs_solicit_adw_key','(FK)  Unique value for each solicitation',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_assoc_merged_previous_id','  The original Associate ID for members being integrated into ACA due to a merger.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_merged_previous_ck_dgt_nbr','  The original Check Digit for members being integrated into ACA due to a merger.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_merged_previous_club_cd','  The original club code for members being integrated into ACA due to a merger.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_merged_previous_key','  The original member key for members being integrated into ACA due to a merger.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_merged_previous_mbr16_id','  The original 16 Digit member ID for members being integrated into ACA due to a merger.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_mbrs_merged_previous_id','  The original membership ID for members being integrated into ACA due to a merger.',CURRENT_DATETIME),"\
                "( 'adw','dim_member','previous_club_cd','  Prior Clubs Code number for an incoming transfer',CURRENT_DATETIME),"\
                "( 'adw','dim_member','previous_club_mbrs_id','  Prior Club Codes Member number for an incoming transfer',CURRENT_DATETIME),"\
                "( 'adw','dim_member','prim_mbr_cd','  Indicates if the member is a P Primary or A Associate',CURRENT_DATETIME),"\
                "( 'adw','dim_member','payment_plan_future_cancel_dt','  The future date the Payment Plan will be canceled',CURRENT_DATETIME)," \
                "( 'adw','dim_member','payment_plan_future_cancel_ind','  This indicates the payment plan has a future cancel datee',CURRENT_DATETIME)," \
                "( 'adw','dim_member','mbr_card_typ_cd','  The description representing the type of card the member has requested',CURRENT_DATETIME),"\
                "( 'adw','dim_member','mbr_card_typ_desc','  The description representing the type of card the member has requested',CURRENT_DATETIME)," \
                "( 'adw','dim_member','entitlement_start_dt','  The beginning date for a Roadside Assistance benefits period.  This is typically a year in which the member is eligible to receive 4 Roadside Assistance calls',CURRENT_DATETIME)," \
                "( 'adw','dim_member','entitlement_end_dt','  The ending date for a Roadside Assistance benefits period.  This is typically a year in which the member is eligible to receive 4 Roadside Assistance calls',CURRENT_DATETIME)," \
                "( 'adw','dim_member','mbr_previous_expiration_dt','  This is the member expiration date on which the members prior membership period ended',CURRENT_DATETIME)," \
                "( 'adw','dim_member','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_member','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_member','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_member','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_member','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_member','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_ar_card_adw_key','(PK)  Unique key for the credit card that is tied to an autorenewal',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','effective_start_datetime','  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','address_adw_key','(FK)  Unique key for each address record.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_arc_status_dtm','  The Date the Auto Renewal Card status was set',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_cc_donor_nbr','  The donor number when the membership is a gift paid for by a donor',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_cc_expiration_dt','  The Expiration date of the Auto Renewal Card',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_cc_last_four','  The last four digits of the of the Auto Renewal Card number',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_cc_reject_dt','  The date the  Auto Renewal Card rejected',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_cc_reject_reason_cd','  The reason the Auto Renewal Card rejected',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_cc_typ_cd','  The type of Credit Card used for the Auto Renewal Card',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_credit_debit_typ_cd','  Indicate if the AutoRenewal card is a Debit Card or Credit Card',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','mbr_source_arc_key','  Source system key for the member auto renewal card key',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','nm_adw_key','(FK)  Unique key for each name record.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_member_auto_renewal_card','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_adw_key','(PK)  Unique value for each rider',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','emp_role_adw_key','(FK)  Unique key for each user role record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_adw_key','(FK)  Unique key for each member in the membership system',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_ar_card_adw_key','(FK)  Unique key for the credit card that is tied to an autorenewal',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_actvtn_dt','  The date the rider became active.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_actual_cancel_dtm','  The date the cancelation process ran',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_billing_category_cd','  The code which indicates the manner in which the riders for the member were priced.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_cancel_dt','  The date the rider cancelled',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_cancel_reason_cd','  The reason the rider was cancelled',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_cost_effective_dt','  The effective date for the most recent dues payment',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_def_key','  The foreign key reference to mz_rider_definition table.  This is not used as the rider_comp_cd is already in the mz_rider table.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_do_not_renew_ind','  Indicates to not renew this rider.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_dues_adj_amt','  The most recent adjustment to the dues.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_dues_cost_amt','  The amount of the most recent dues paid.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_effective_dt','  The date the rider became effective.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_extend_exp_amt','  Indicate the  number of months the membership expiration date was extended when reinstated',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_future_cancel_dt','  The future date a membership will be cancelled when not renewing.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_original_cost_amt','  The original dues cost before adjustments',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_paid_by_cd','  Indicate if the P Primary Member of D Donor paid for the rider.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_payment_amt','  The amount paid toward the dues amount',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_reinstate_ind','  Indicate the lapsed rider was reinstated',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_reinstate_reason_cd','  The reason the rider was reinstated',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_solicit_cd','  The solicitation (creative) which the member used to join AAA.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_source_key','  The primary table key for mz_rider',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_status_cd','  The rider status',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbr_rider_status_dtm','  The date the rider became active.',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','product_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_member_rider','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_adw_key','(PK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','aca_office_adw_key','(FK)  Unique Key for each ACA store record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','address_adw_key','(FK)  Unique key for each address record.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_aaawld_online_ed_ind','  This indicates that the member wants to use the AA World On-line edition.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_address_change_dtm','  The date the address changed in MzP',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_address_ncoa_update_ind','  Indicate the address was updated due to a National Change of Address file',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_assigned_retention_ind','  Indicate the membership was assigned to a Retention agent',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_bad_address_ind','  Indicate if the address is undeliverable by the post office,',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_billing_category_cd','  This is the Billing Category Code largely used to set pricing,  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_billing_cd','  The code indicating how the most recent billing was created.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_cancel_dt','  The date the membership cancelled.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_cdx_export_update_dtm','  The date fields in the CDX export changed.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dn_direct_mail_slc_ind','  Indicates the member does not want to receive direct mail solicitations.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dn_offer_plus_ind','  Indicates to not add on plus to this membership',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dn_salvage_ind','  Indicates a lapsed member should not be salvaged',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dn_send_aaawld_ind','  Indicates the member does not want to receive AAA World',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dn_send_pubs_ind','  Indicates the members does not want to receive AAA World mailings.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dn_solicit_ind','  Indicate is the member has requested to be sent marketing mailings,.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dn_telemkt_ind','  Indicates the member does not want to receive telemarketing calls',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dues_adj_amt','  The amount the dues was adjusted for all active riders on the membership.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_dues_cost_amt','  The dues cost for all active riders on the membership.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_ebill_ind','  Indicates if the member wants to receive ebills.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_ers_abuser_ind','  Indicates the member is an ERS Heavy User',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_future_cancel_dt','  The future date a membership will be cancelled. ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_future_cancel_ind','  Indicates if the membership will be cancelled in a future ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_group_cd','  The Group membership code indicating the group to which the members belongs.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_heavy_ers_ind','  Indicates the member is an ERS Heavy User',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_id','  7 digit Membership ID',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_market_tracking_cd','  The tracking code designating the market the member lives in.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_marketing_segmntn_cd','  Indicates the segmentation code applied to the membership for various marketing panels.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_mbrs_no_promotion_ind','  Indicates the member does not want to receive membership promotion mailing such as Add on Associate.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_out_of_territory_cd','  Indicates if the members address is in territory or transferring out of territory.  I = In Territory, O = Out of Territory, T = Transferring out of Territory, D = Do not Transfer.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_phone_typ_cd','  The type of phone for the number referenced in the phone field.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_promo_cd','  The promotional code the member responded to when joining AAA.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_purge_ind','  Indicates if this membership was purged',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_unapplied_amt','  The amount that was not applied on the Membership',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_advance_payment_amt','  The amount Membership made as over payment',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_balance_amt','  Membership balance amount',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','payment_plan_future_cancel_dt','  The future date the Payment Plan will be canceled',CURRENT_DATETIME)," \
                "( 'adw','dim_membership','payment_plan_future_cancel_ind','  This indicates the payment plan has a future cancel date',CURRENT_DATETIME)," \
                "( 'adw','dim_membership','switch_prim_ind','  This indicates if the primary and associate on this membership were switched',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','supress_bill_ind','  This indicates if the member wants to have all bills except the 1st bill to be suppressed',CURRENT_DATETIME)," \
                "( 'adw','dim_membership','offer_end_dt','  This indicates the last date the offer the membership was sold under will end',CURRENT_DATETIME)," \
                "( 'adw','dim_membership','old_payment_plan_ind','  Indicates if this payment plan is under and old plan',CURRENT_DATETIME)," \
                "( 'adw','dim_membership','carry_over_amt','  The amount of membership balance carried over from the most recent payment plan billing',CURRENT_DATETIME)," \
                "( 'adw','dim_membership','mbrs_salvage_ind','  Indicates the membership has marked for a salvage campaign',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_salvaged_ind','  Indicates the member was salvaged',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_shadow_mcycle_ind','  Indicates that motorcycle coverage is automatically included along with the Classic coverage thus suppressing the extra $15 motorcycle classic charge.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_shadowrv_coverage_ind','  Indicates that RV coverage is automatically included along with the Plus coverage thus suppressing the extra $15 RV charge.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_source_system_key','  Key in MZP for this membership',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_status_cd','  The current status of the membership',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_status_dtm','  The most recent date the status of the membership was set.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_student_ind','  Indicate the member is  full time student',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_temporary_address_ind','  Indicates if the membership is a temporary address.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_tier_key','  The key indicating the tier to which the membership belongs.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_transfer_club_cd','  The club to which a membership is transferring to.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_transfer_dtm','  The date a membership transferred to a different club',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_typ_cd','  This code represents special membership types such as family memberships.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_merged_previous_club_cd','  Prior Club Code for a Membership acquired through merger',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_mbrs_merged_previous_id','  Prior Membership Identifier for a Membership acquired through merger',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','mbrs_mbrs_merged_previous_key','  Prior Membership Key for a Membership acquired through merger',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','phone_adw_key','(FK)  Unique key for each phone record.',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','previous_club_cd','  Prior Clubs Code number for an incoming transfer',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','previous_club_mbrs_ind','  Prior Club Codes Member number for an incoming transfer',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','product_adw_key','(FK)  Unique key for the product record that ties to the coverage level code',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_membership','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','mbrs_fee_adw_key','(PK)  Unique key for each membership fee',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','donor_nbr','  The donor who is paying the fee for a donated membership.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','fee_dt','  The date the fee has been applied.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','fee_typ','  The type of fee such as Enrollment fee and same day service fee.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','mbr_adw_key','(FK)  Unique key for each member in the membership system',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','mbrs_fee_source_key','  Source key from the membership fees table',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','product_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','status','  The status of the fee record.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','waived_by','  The employee who waived the fee.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','waived_dt','  The date the fee was waived.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','waived_reason_cd','  The reason the fee was waived.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_fee','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','mbrs_marketing_segmntn_adw_key','(PK)  Unique key for each membership that is added to a distinct marketing segment',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','effective_start_datetime','  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','mbr_expiration_dt','  The expiration date to which the segment applies',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','segment_nm','  The name of the segment',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','segmntn_comm_cd','  The commission code to which the Segment applies',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','segmntn_control_panel_ind','  Indicate if this is Control Panel',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','segmntn_panel_cd','  This code is used to populate the segmentation code in mz_membership.',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','segmntn_test_group_cd','  Indicate if the panel is C (Control) T (Test)',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_mbrs_marketing_segmntn','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','mbrs_payment_plan_adw_key','(PK)  Unique value that is assigned to a payment plan for a member rider',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','effective_start_datetime','  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','mbr_rider_adw_key','(FK)  Unique value for each rider',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_nbr','  The sequential number of the current payment record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_plan_charge_dt','  The date the payment plan charge was applied',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_plan_month','  the duration in months over which the payment plan is spread',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_plan_nm','  The name of the payment plan',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_plan_payment_cnt','  the number of payments for the duration of the payment plan',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_plan_source_key','  The source system key from which this record is coming from.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_plan_status','  The status of the current payment plan for the membership',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','safety_fund_donation_ind','  This indicates if the payment record is for AAA National Safety Fund.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','payment_amt','  The amount of the installment payment for the payment plan.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_payment_plan','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','mbrs_solicit_adw_key','(PK)  Unique value for each solicitation',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','effective_start_datetime','  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','mbr_typ_cd','  Indicate if the solicitation applies to Primary, Associate or both member types',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','mbrs_solicit_source_system_key','  Source System Key unique to this solicitation',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','solicit_camp_cd','  The same code as the solicitation_cd',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','solicit_category_cd','  Unused all records are null',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','solicit_cd','  The solicitation code',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','solicit_discount_cd','  The discount associated with the solicitation',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','solicit_group_cd','  The code for Group membership to which the solicitation applies',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_membership_solicitation','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','nm_adw_key','(PK)  Unique key for each name record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','cleansed_first_nm','  First name after cleansing applied that is utilized in matching',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','cleansed_last_nm','  Last name after cleansing applied that is utilized in matching',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','cleansed_middle_nm','  Middle Name after Cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','cleansed_suffix_nm','  Suffix name after cleansing applied that is utilized in matching',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','cleansed_title_nm','  Title after cleansing applied',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','nm_valid_status_cd','  This field indicates if the cleansed name values tie out to a verified name',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','nm_valid_typ','  This field describes the type of process that has been used to validate and/or enhance the name data',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','raw_first_nm','  Raw First Name from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','raw_last_nm','  Raw Last Name from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','raw_middle_nm','  Raw Middle name or initial from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','raw_suffix_nm','  Raw Name Suffix from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','raw_title_nm','  Raw Title from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_name','integrate_update_batch_number','  Batch number of the most recent update to this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','phone_adw_key','(PK)  Unique key for each phone record.',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','cleansed_ph_area_cd_nbr','  First three digits of the phone number not counting the country code after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','cleansed_ph_extension_nbr','  Phone number extension where applicable after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','cleansed_ph_nbr','  Formatted phone number',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','cleansed_ph_prefix_nbr','  Middle three digits of the phone number after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','cleansed_ph_suffix_nbr','  Last four digits of the phone number after cleansing',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','phone_valid_status_cd','  This field indicates if the cleansed phone values tie out to a verified phone',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','phone_valid_typ','  This field describes the type of process that has been used to validate and/or enhance the phone data',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','raw_phone_nbr','  Raw phone number from source',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','integrate_update_datetime','  Datetime that the latest update to this record occurred',CURRENT_DATETIME),"\
                "( 'adw_pii','dim_phone','integrate_update_batch_number','  Batch Number of the most recent load to update this record',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_adw_key','(PK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','dim_product','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_product','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_category_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_dynamic_sku','  An additional SKU indicating the options being loaded onto a an product.  Examples of this a single Disney pass represented by a SKU could have entrance passes, meal plans and parking loaded onto a single Disney Card.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_dynamic_sku_desc','  The description of the dynamic SKU such as Disney 5 day entrance pass, parking or Disney meal plan. ',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_dynamic_sku_key','  The source system key related to the dynamic sku.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_item_desc','  The textual item description.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_item_nbr_cd','  The item for the item.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_service_category_cd','  This categorizes the product be type of service.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_sku','  The bar code or unique product code uniquely identifying a product or service provided to the customer.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_sku_desc','  The textual description of the product or service provided to the customer.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_sku_effective_dt','  The first date the product or service is available.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_sku_expiration_dt','  The final date the product or service is available.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_sku_key','  The source system code uniquely identifying a product or service provided to the customer.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_status_cd','  The code indicating that the member is active and available.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_unit_cost','  ACAs current cost of the product.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','product_vendor_nm','  The vendor name providing the product or service for the customer.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','vendor_adw_key','  Unique key relating the vendor providing the product or service for the customer.',CURRENT_DATETIME),"\
                "( 'adw','dim_product','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_product','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_product','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_product','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_product','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_product','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','product_category_adw_key','(PK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','biz_line_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','product_category_cd','  The code representing the Product Category.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','product_category_desc','  The description of a Product Category.  ',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','product_category_status_cd','  The status of the Product Category.',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','dim_product_category','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','dim_vendor','vendor_adw_key','(PK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','biz_line_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','aaa_facility','  Indicates if this is a Club Owned Roadside Assistance Facility.  ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','contact_adw_key','(FK)  This represents vendor name, physical address, email address and phone number for the vendor.  ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','fleet_ind','  This indicates if the fleet ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','internal_facility_cd','  The Roadside assistance facility identifier.  ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','preferred_vendor','  The description if this vendor is a vendor ACA /  AAA prefers to do business with.  Examples are Preferred, Diamond, Non Preferred. ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','service_region','  This is particular to Roadside Assistance indicating the region is which the service facility serves the customer. ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','service_region_desc','  This is a description of the particular to Roadside Assistance indicating the region is which the service facility serves the customer. ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','vendor_cd','  The code identifying the vendor.',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','vendor_contact','  This is the primary contact for the vendor.  ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','vendor_status','  This indicates if we current use products or services from this vendor.  ',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','vendor_typ','  The type of the vendor.  Examples are Roadside Assistance Facility, Retail Merchandise vendor and Insurance Carrier.',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','Dim_vendor','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','mbrs_billing_detail_adw_key','(PK)  Unique value for each bill line item detail',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','bill_detail_amt','  The dues amount for the rider component being billed.',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','bill_detail_source_key','  Primary key from the Bill Detail source table',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','bill_detail_text','  The text that will be displayed on the billing statement.  This is derived from rider component code or fee types.  ',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','bill_notice_nbr','  The sequential Notice Number related to the Expiration Date and Bill Type',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','bill_process_dt','  The date the bill was processed',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','bill_summary_source_key','  The mz_bill_summary_key which joins to the mz_bill_detail table',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','bill_typ','  The type of billing.  ',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','mbr_adw_key','(FK)  Unique key for each member in the membership system',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','mbrs_billing_summary_adw_key','(FK)  Unique key for each instance of a bill being sent out',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','posted_user_adw_key','(FK)  Unique key for each instance of a bill being sent out',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','product_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','rider_billing_category_cd','  The Billing Category Code of a rider',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','rider_billing_category_desc','  The description of Billing Category Code of a rider',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','rider_solicit_cd','  The solicitation code applied to the billing detail record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_detail','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','mbrs_billing_summary_adw_key','(PK)  Unique key for each instance of a bill being sent out',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','effective_start_datetime','  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_amt','  The total amount of the bill net of discounts and credits',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_credit_amt','  The total amount of credits on the bill',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_ebilling_ind','  Indicates if the bill was emailed instead of mailed',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_notice_nbr','  The sequential Notice Number related to the Expiration Date and Bill Type',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_paid_amt','  The Payment amount applied to the billing.',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_panel_cd','  The panel which the bill is associated.',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_process_dt','  The date the bill was processed',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_renewal_method','  Distinguishes if the bill will be charged by Auto Renewal.',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_summary_source_key','  The mz_bill_summary_key which joins to the mz_bill_detail table',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','bill_typ','  The type of billing.  ',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','mbr_expiration_dt','  The date the member will expire',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','membership_billing_summary','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','mbrs_gl_payments_applied_adw_key','(PK)  Unique key for each GL Payment applied record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','aca_office_adw_key','(FK)  Unique Key for each ACA store record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_account_nbr','  The GL Code',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_gl_desc','  The description of the GL code',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_journal_amt','  The amount of the Journal Entry',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_journal_created_dtm','  The date the Journal Entry was created',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_journ_d_c_cd','  Indicates if the Journal Entry is a Debit or Credit',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_journal_source_key','  Primary key from Journal source table',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_post_dt','  The date the Journal Entry was applied',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','gl_payment_process_dt','  The date the Journal Entry was processed',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','mbrs_payment_applied_summary_adw_key','(FK)  Unique Key for each payment applied to a membership',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','mbrs_gl_payments_applied','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','payment_apd_dtl_source_key','  The key from the mz_payment_detail table from which this record was sourced.  ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','discount_amt','  The planned amount of the Discount',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','discount_effective_dt','  The date the discount become effective',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','mbr_adw_key','(FK)  Unique key for each member in the membership system',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','mbrs_payment_apd_dtl_adw_key','(PK)  Unique key for each detail line of the payment being applied to a membership',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','mbrs_payment_applied_summary_adw_key','(FK)  Unique Key for each payment applied to a membership',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','payment_applied_dt','  The date the payment was applied',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','pymt_apd_discount_counted','  Indicate is the Discount is counted',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','payment_applied_unapplied_amt','  The amount of previous unapplied payments applied to this line item',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','payment_detail_amt','  The payment amount applied to this line item',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','payment_method_cd','  The payment method code used to identify if the funds are check, credit card, debit card etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','payment_method_desc','  The payment method such as Check, Cash, Credit Card etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','product_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','rider_cost_effective_dtm','  The first day of the membership period the payment applies',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_detail','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','mbrs_payment_applied_summary_adw_key','(PK)  Unique Key for each payment applied to a membership',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','aca_office_adw_key','(FK)  Unique Key for each ACA store record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','emp_adw_key','(FK)  Unique key for each ACA User record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','mbrs_payment_apd_rev_adw_key','(FK)  Key that ties a payment applied to the payment it is reversing',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','mbrs_payment_received_adw_key','(FK)  Unique key for each Payment received ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_adj_cd','  The method of payment adjustments',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_adj_desc','  The description of the method of payment adjustments',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_applied_amt','  The payment amount applied',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_applied_dt','  The date the payment was applied',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_applied_source_key','  The source key from mz_payment_summary from which the record came from.  ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_batch_nm','  The name of the Batch',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_created_dtm','  The date the payment record was created',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_method_cd','  The payment method code used to identify if the funds are check, credit card, debit card etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_method_desc','  The payment method such as Check, Cash, Credit Card etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_paid_by_cd','  Indicate is the payment is being paid by a Primary member, Associate member or Donor',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_typ_cd','  The type of transaction for the payment',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_typ_desc','  The description of the type of transaction for the payment',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_unapplied_amt','  The amount of an overpayment applied to Unapplied Cash to be applied at a later time',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','advance_payment_amt','This field is used to record over payments on active memberships where the funds will be held on the membership for future dues payments.  ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_source_cd','This is the code for the source of the payment such as (MM) Membership Maintenance, (ME) Membership Entry , (AR) Auto Renewal etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_source_desc','This is the description for the source of the payment such as Membership Maintenance, Membership Entry, (AR) Auto Renewal etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_tran_typ_cd','The code representing the transaction type such as PMT (Payment) and CAN (Cancellation). ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','payment_tran_typ_cdesc','This is the description of the code representing the transaction type such as Payment and Cancellation',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_applied_summary','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','mbrs_payment_received_adw_key','(PK)  Unique key for each Payment received ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','aca_office_adw_key','(FK)  Unique Key for each ACA store record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','emp_adw_key','(FK)  Unique key for each ACA User record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','mbr_ar_card_adw_key','(FK)  Unique key for the credit card that is tied to an autorenewal',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','membership_billing_summary_adw_key','(FK)  Unique key for each instance of a bill being sent out',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_adj_cd','  The method of payment adjustments',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_adj_desc','  The description method of payment adjustments',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_paid_by_desc','  Indicate is the payment is being paid by a Primary member, Associate member or Donor',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_reason_cd','  The reason the batch payment record was created.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_amt','  The amount of the payment received ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_ar_ind','  Indicates if this payment was due to an Auto Renewal',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_batch_key','  The payment batch key from which the record was generated.  ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_cnt','  The expected number of payments within the batch',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_created_dtm','  The date the batch is created',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_expected_amt','  The expected total of payments within the batch',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_method_cd','  The payment method code used to identify if the funds are check, credit card, debit card etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_method_desc','  The payment method such as Check, Cash, Credit Card etc.',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_nm','  the name of the batch payment.  ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_post_dtm','  The date the payment was posted',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_post_fl','  Indicates that the payment was posted',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_source_key','  The table key from mz_batch_payment from which this record was sourced.  ',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_status','  The status of the batch',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_received_status_dtm','  The date the current status was set',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_source_cd','  The source of the payment',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_source_desc','  The description of source of the payment',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_typ_cd','  The type of transaction for the payment',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','payment_typ_desc','  The description of the type of transaction for the payment',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','mbrs_payment_received','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_adw_key','(PK)  Unique key for each sales agent activity record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','effective_start_datetime','(FK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','aca_membership_office_adw_key','(FK)  Unique Key for each ACA store record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','aca_sales_office','(FK)  Unique Key for each ACA store record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','daily_cnt','  The summarized metric derived from the Transaction Code, Indicator and flag fields.',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','emp_role_adw_key','(FK)  Unique key for each user role record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbr_adw_key','(FK)  Unique key for each member in the membership system',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbr_comm_cd','  The commission code denoting if the transaction code is new, renewal, transfer of drop recovery',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbr_comm_desc','  The commission code denoting if the transaction code is new, renewal, transfer of drop recovery',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbr_expiration_dt','  The date which this rider will expire',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbr_rider_cd','  The rider to which this transaction applies',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbr_rider_desc','  The rider to which this transaction applies',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbr_typ_cd','  Designates if the member is a Primary or Associate',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbrs_adw_key','(FK)  Unique key for each Membership record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbrs_payment_applied_detail_adw_key','(FK)  Unique key for each detail line of the payment being applied to a membership',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','mbrs_solicit_adw_key','(FK)  Unique value for each solicitation',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','product_adw_key','(FK)  Unique key for the product record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','rider_billing_category_cd','  The billing category code used to set the pricing for this transaction.',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','rider_billing_category_desc','  The billing category code description used to set the pricing for this transaction.',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','rider_solicit_cd','  The solicitation code associated with this transaction',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','rider_source_of_sale_cd','  The source of sale from which this transaction originated',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','safety_fund_donation_ind','  This indicates if the payment record is for AAA National Safety Fund.  ',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_add_on_ind','  Indicates if the rider is being added onto an existing membership',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_ar_ind','  Indicates if the record is the result of an Auto Renewal payment',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_commsable_activity_ind','  Indicates if the agent for this record is eligible for commission',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_daily_billed_ind','  Indicates if this was the result of a daily bill.',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_daily_cnt_ind','  This indicates that this record will be counted for sales and charge backs.  ',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_dues_amt','  The dues amount being applied to this record.',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_fee_typ_cd','  The foreign key to the cx_codes indicating the type of Fee being charged in excess of dues',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_payment_plan_ind','  Indicates if the records are related to a payment plan',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_source_key','  The primary table key which is referenced in other tables such as Payment Detail, Billing Detail etc.',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_source_nm','  This indicates the source table from which the transaction is coming from mz_sales_agent_activity or mz_daily_count_summary',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','saa_tran_dt','  The Date of the transaction',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','sales_agent_tran_cd','  The transaction code indicating the type of transaction, the rider and the member type.',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','zip_cd','  The zip code in which the membership was in at the time of the transaction',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','adw_row_hash','  Hash Key of appropriate fields to enable easier type 2 logic',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','sales_agent_activity','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','contact_adw_key','(PK)(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','contact_source_system_nm','(PK)  Name of the source system that this record was generated in',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','address_typ_cd','(PK)  Name of the type of address that will be stored',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','address_adw_key','(FK)  Unique key for each address record.',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_address','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','contact_adw_key','(PK)(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','contact_source_system_nm','(PK)  Name of the source system that this record was generated in',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','email_typ_cd','(PK)  Name of the type of email address that will be stored',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','email_adw_key','(FK)  Unique key for each email record.',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_email','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','contact_adw_key','(PK)(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','contact_source_system_nm','(PK)  Name of the source system that this record was generated in',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','nm_typ_cd','(PK)  Name of the type of name that will be stored',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','nm_adw_key','(FK)  Unique key for each name record.',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_nm','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','contact_adw_key','(PK)(FK)  Unique key created for each contact info record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','contact_source_system_nm','(PK)  Name of the source system that this record was generated in',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','phone_typ_cd','(PK)  Name of the type of phone number that will be stored',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','effective_start_datetime','(PK)  Date that this record is effectively starting',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','effective_end_datetime','  Date that this record is effectively ending',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','phone_adw_key','(FK)  Unique key for each phone record.',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','actv_ind','  Indicates if the record is currently the active record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','integrate_insert_datetime','  Datetime that this record was first inserted',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','integrate_insert_batch_number','  Batch that first inserted this record',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','integrate_update_datetime','  Most recent datetime that this record was updated',CURRENT_DATETIME),"\
                "( 'adw','xref_contact_phone','integrate_update_batch_number','  Most recent batch that updated this record',CURRENT_DATETIME)"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 19),
    'email': [FAIL_EMAIL],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360),
    'params': {
        'dag_name': DAG_TITLE
    }
}

with DAG('UTIL-FIXED-DATA-INSERTION', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    delete_dim_product_category = BigQueryOperator(
        task_id='delete_dim_product_category',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` WHERE product_category_adw_key in ('574ad6f5-1233-42ff-bb2b-ebda5e204c31','48124364-3e3e-4194-8880-8ddcdfb5acc9','eefce1e6-3fee-4ba2-a19c-0e54b29abc72')",
        use_legacy_sql=False)
    delete_dim_business_line = BigQueryOperator(
        task_id='delete_dim_business_line',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_business_line` WHERE 1=1",
        use_legacy_sql=False)
    delete_dim_vendor = BigQueryOperator(
        task_id='delete_dim_vendor',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor` WHERE vendor_adw_key = '6e3dfbd4-2ce0-41a1-ade9-6407d1c2fe52'",
        use_legacy_sql=False)
    delete_dim_product = BigQueryOperator(
        task_id='delete_dim_product',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` WHERE product_adw_key = 'ec4d187f-ed53-4d12-890e-a9a848c5d42e'",
        use_legacy_sql=False)
    delete_dim_office = BigQueryOperator(
        task_id='delete_dim_office',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` WHERE aca_office_adw_key = '-2'",
        use_legacy_sql=False)
    delete_data_steward = BigQueryOperator(
        task_id='delete_data_steward',
        bql="DELETE FROM `{{var.value.INGESTION_PROJECT}}.admin.data_steward` WHERE 1=1",
        use_legacy_sql=False)
    delete_dim_state = BigQueryOperator(
        task_id='delete_dim_state',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_state` WHERE 1=1",
        use_legacy_sql=False)


    dim_product_category = BigQueryOperator(
        task_id='dim_product_category',
        bql=dim_product_category_sql,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_business_line = BigQueryOperator(
        task_id='dim_business_line',
        bql=dim_business_line,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_vendor = BigQueryOperator(
        task_id='dim_vendor',
        bql=dim_vendor,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_product = BigQueryOperator(
        task_id='dim_product',
        bql=dim_product,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND')
    dim_office = BigQueryOperator(
        task_id='dim_office',
        bql=dim_office,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND')
    stubbed_dims = BigQueryOperator(
        task_id='stubbed_dims',
        bql='sql/stubbed_data/populate_stubbed_records.sql',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND')
    data_steward_desc=BigQueryOperator(
        task_id='data_steward',
        bql=data_steward,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_state = BigQueryOperator(
        task_id='dim_state',
        bql='sql/stubbed_data/populate_dim_state.sql',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')

delete_dim_state >> dim_state >> stubbed_dims
delete_dim_product_category >> dim_product_category >> stubbed_dims
delete_dim_business_line >> dim_business_line >> stubbed_dims
delete_dim_vendor >> dim_vendor >> stubbed_dims
delete_dim_product >> dim_product >> stubbed_dims
delete_dim_office >> dim_office >> stubbed_dims
stubbed_dims >> delete_data_steward >> data_steward_desc
    
AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([data_steward_desc])