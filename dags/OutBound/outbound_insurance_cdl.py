#######################################################################################################################
##  DAG_NAME :  outbound_insurance_cdl
##  PURPOSE :   Executes Outsbound DAG for CDL insurance
##  PREREQUISITE DAGs :
#######################################################################################################################


from airflow.utils.dates import timezone
import pendulum
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

local_tz = pendulum.timezone("US/Eastern")


config_tables = {
      'fact_roadside_service_call|rs_service_call_adw_key|integrate_update_datetime':
    {
    'rs_service_call_adw_key,contact_adw_key,product_adw_key,office_commcenter_adw_key,svc_fac_vendor_adw_key,ers_truck_adw_key,ers_drvr_adw_key,archive_dtm,archive_adw_key_dt,service_call_dtm,service_adw_key_dt,call_id,call_status_cd,call_status_desc,call_status_reason_cd,call_status_reason_desc,call_status_detail_reason_cd,call_status_detail_reason_desc,call_reassignment_cd,call_reassignment_desc,call_source_cd,call_source_desc,first_problem_cd,first_problem_desc,final_problem_cd,final_problem_desc,first_tlc_cd,first_tlc_desc,final_tlc_cd,final_tlc_desc,vehicle_year,vehicle_make,vehicle_model,promised_wait_tm,loc_breakdown,tow_dest,total_call_cost,fleet_adjusted_cost,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number':"rs_service_call_adw_key,contact_adw_key,product_adw_key,office_commcenter_adw_key,svc_fac_vendor_adw_key,ers_truck_adw_key,ers_drvr_adw_key,ifnull(safe_cast( if( archive_dtm < '1900-01-01', null, archive_dtm) as string),'null') as archive_dtm,archive_adw_key_dt,ifnull(safe_cast( if( service_call_dtm < '1900-01-01', null, service_call_dtm) as string),'null') as service_call_dtm,service_adw_key_dt,call_id,call_status_cd,call_status_desc,call_status_reason_cd,call_status_reason_desc,call_status_detail_reason_cd,call_status_detail_reason_desc,call_reassignment_cd,call_reassignment_desc,call_source_cd,call_source_desc,first_problem_cd,first_problem_desc,final_problem_cd,final_problem_desc,first_tlc_cd,first_tlc_desc,final_tlc_cd,final_tlc_desc,vehicle_year,vehicle_make,vehicle_model,promised_wait_tm,loc_breakdown,tow_dest,total_call_cost,fleet_adjusted_cost,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number"
    } ,
    'insurance_customer_role|contact_adw_key, ins_policy_adw_key, ins_rel_type|integrate_update_datetime':
    {
    'contact_adw_key,ins_policy_adw_key,ins_rel_type,drvr_ins_number,drvr_ins_birth_dt,drvr_ins_good_student_cd,drvr_ins_good_student_desc,drvr_ins_marital_status_cd,drvr_ins_marital_status_desc,drvr_license_state,effective_start_datetime,effective_end_datetime,rel_effective_start_datetime,rel_effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number':"contact_adw_key,ins_policy_adw_key,ins_rel_type,drvr_ins_number,ifnull(safe_cast( if( drvr_ins_birth_dt < '1900-01-01', null, drvr_ins_birth_dt) as string),'null') as drvr_ins_birth_dt,drvr_ins_good_student_cd,drvr_ins_good_student_desc,drvr_ins_marital_status_cd,drvr_ins_marital_status_desc,drvr_license_state,effective_start_datetime,effective_end_datetime,ifnull(safe_cast( if( rel_effective_start_datetime < '1900-01-01', null, rel_effective_start_datetime) as string),'null') as rel_effective_start_datetime,ifnull(safe_cast( if( rel_effective_end_datetime < '1900-01-01', null, rel_effective_end_datetime) as string),'null') as rel_effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number"
    },
    'dim_demo_ins_score|demo_ins_score_adw_key|integrate_insert_datetime':
    {
     'demo_ins_score_adw_key, demo_person_adw_key,demo_activity_tile,demo_channel_pref_i_dcil,demo_channel_pref_d_dcil,demo_channel_pref_e_dcil,demo_channel_pref,demo_group_cd,demo_life_attrin_mdl_dcil,demo_mkt_rsk_clsfir_auto_dcil,demo_life_class_dcil,demo_premium_dcil,demo_prospect_survival_dcil,demo_pred_rnw_month_auto,demo_pred_rnw_month_home,actv_ind,integrate_snapshot_dt,integrate_insert_datetime,integrate_insert_batch_number':"demo_ins_score_adw_key, demo_person_adw_key, demo_activity_tile, demo_channel_pref_i_dcil, demo_channel_pref_d_dcil, demo_channel_pref_e_dcil, demo_channel_pref, demo_group_cd, demo_life_attrin_mdl_dcil, demo_mkt_rsk_clsfir_auto_dcil, demo_life_class_dcil, demo_premium_dcil, demo_prospect_survival_dcil, demo_pred_rnw_month_auto, demo_pred_rnw_month_home, actv_ind, ifnull(safe_cast( if( integrate_snapshot_dt < '1900-01-01', null, integrate_snapshot_dt) as string),'null') as integrate_snapshot_dt, integrate_insert_datetime, integrate_insert_batch_number"
    },
    'insurance_policy|ins_policy_adw_key|integrate_update_datetime':
    {
     'ins_policy_adw_key,channel_adw_key ,biz_line_adw_key,product_category_adw_key,ins_quote_adw_key,state_adw_key ,emp_adw_key ,ins_policy_system_source_key ,ins_policy_quote_ind ,ins_policy_number,ins_policy_effective_dt ,ins_policy_expiration_dt,ins_policy_cntrctd_exp_dt,ins_policy_annualized_comm ,ins_policy_annualized_premium ,ins_policy_billed_comm,ins_policy_billed_premium ,ins_policy_estimated_comm ,ins_policy_estimated_premium ,effective_start_datetime ,effective_end_datetime,actv_ind ,adw_row_hash ,integrate_insert_datetime ,integrate_insert_batch_number ,integrate_update_datetime ,integrate_update_batch_number': "ins_policy_adw_key,channel_adw_key ,biz_line_adw_key,product_category_adw_key,ins_quote_adw_key,state_adw_key ,emp_adw_key ,ins_policy_system_source_key ,ins_policy_quote_ind ,ins_policy_number,ifnull(safe_cast( if( ins_policy_effective_dt < '1900-01-01', null, ins_policy_effective_dt) as string),'null') as ins_policy_effective_dt,ifnull(safe_cast( if( ins_policy_expiration_dt < '1900-01-01', null, ins_policy_expiration_dt) as string),'null') as ins_policy_expiration_dt,ifnull(safe_cast( if( ins_policy_cntrctd_exp_dt < '1900-01-01', null, ins_policy_cntrctd_exp_dt) as string),'null') as ins_policy_cntrctd_exp_dt,ins_policy_annualized_comm ,ins_policy_annualized_premium ,ins_policy_billed_comm,ins_policy_billed_premium ,ins_policy_estimated_comm ,ins_policy_estimated_premium ,effective_start_datetime ,effective_end_datetime,actv_ind ,adw_row_hash ,integrate_insert_datetime ,integrate_insert_batch_number ,integrate_update_datetime ,integrate_update_batch_number"
    },
    'fact_sales_header|sales_header_adw_key|integrate_update_datetime':
    {
     'sales_header_adw_key,aca_office_adw_key,emp_adw_key,contact_adw_key,source_system,source_system_key,sale_adw_key_dt,sale_dtm,receipt_nbr,sale_void_ind,return_ind,total_sale_price,total_labor_cost,total_cost,total_tax,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number': "sales_header_adw_key,aca_office_adw_key,emp_adw_key,contact_adw_key,source_system,source_system_key,sale_adw_key_dt,ifnull(safe_cast( if( sale_dtm < '1900-01-01', null, sale_dtm) as string),'null') as sale_dtm,receipt_nbr,sale_void_ind,return_ind,total_sale_price,total_labor_cost,total_cost,total_tax,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number"
    },
    'fact_sales_detail|fact_sales_detail_adw_key|integrate_update_datetime':
    {
    'fact_sales_detail_adw_key,sales_header_adw_key,product_adw_key,biz_line_adw_key,line_item_nbr,item_cd,item_desc,item_category_cd,item_category_desc,quantity_sold,unit_sale_price,unit_labor_cost,unit_item_cost,extended_sale_price,extended_labor_cost,extended_item_cost,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number': "fact_sales_detail_adw_key,sales_header_adw_key,product_adw_key,biz_line_adw_key,line_item_nbr,item_cd,item_desc,item_category_cd,item_category_desc,quantity_sold,unit_sale_price,unit_labor_cost,unit_item_cost,extended_sale_price,extended_labor_cost,extended_item_cost,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number"
    },
    'insurance_policy_line|ins_line_adw_key|integrate_update_datetime':
    {
    'ins_line_adw_key ,ofc_aca_adw_key, premium_paid_vendor_key, legal_issuing_vendor_key, product_adw_key, ins_policy_adw_key, ins_policy_line_source_key, ins_line_comm_percent, ins_line_delivery_method_cd, ins_line_delivery_method_desc, ins_line_effective_dt, ins_line_expiration_dt, ins_line_first_written_dt, ins_line_status, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number':"ins_line_adw_key ,ofc_aca_adw_key,premium_paid_vendor_key,legal_issuing_vendor_key,product_adw_key,ins_policy_adw_key ,ins_policy_line_source_key,ins_line_comm_percent,ins_line_delivery_method_cd,ins_line_delivery_method_desc,ifnull(safe_cast( if( ins_line_effective_dt < '1900-01-01', null, ins_line_effective_dt) as string),'null') as ins_line_effective_dt,ifnull(safe_cast( if( ins_line_expiration_dt < '1900-01-01', null, ins_line_expiration_dt)as string),'null') as ins_line_expiration_dt,ifnull(safe_cast( if( ins_line_first_written_dt < '1900-01-01', null, ins_line_first_written_dt) as string),'null') as ins_line_first_written_dt,ins_line_status,effective_start_datetime,effective_end_datetime ,actv_ind    ,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number"
    },
	#'dim_aca_office|aca_office_adw_key|NULL':
	#{
	# 'aca_office_adw_key,aca_office_list_source_key,aca_office_address,aca_office_state_cd,aca_office_phone_nbr,aca_office_nm,aca_office_status,aca_office_typ,mbrs_branch_cd,ins_department_cd,rs_asstc_communicator_center,car_care_loc_nbr,tvl_branch_cd,aca_office_division,retail_sales_office_cd,retail_manager,retail_mngr_email,retail_assistant_manager,retail_assistant_mngr_email,staff_assistant,staff_assistant_email,car_care_manager,car_care_mngr_email,car_care_assistant_manager,car_care_assistant_mngr_email,region_tvl_sales_mgr,region_tvl_sales_mgr_email,retail_sales_coach,retail_sales_coach_email,xactly_retail_store_nbr,xactly_car_care_store_nbr,region,car_care_general_manager,car_care_general_mngr_email,car_care_dirctr,car_care_dirctr_email,car_care_district_nbr,car_care_cost_center_nbr,tire_cost_center_nbr,admin_cost_center_nbr,occupancy_cost_center_nbr,retail_general_manager,retail_general_mngr_email,district_manager,district_mngr_email,retail_district_nbr,retail_cost_center_nbr,tvl_cost_center_nbr,mbrs_cost_center_nbr,facility_cost_center_nbr,national_ats_nbr,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number': "aca_office_adw_key,aca_office_list_source_key,aca_office_address,aca_office_state_cd,aca_office_phone_nbr,aca_office_nm,aca_office_status,aca_office_typ,mbrs_branch_cd,ins_department_cd,rs_asstc_communicator_center,car_care_loc_nbr,tvl_branch_cd,aca_office_division,retail_sales_office_cd,retail_manager,retail_mngr_email,retail_assistant_manager,retail_assistant_mngr_email,staff_assistant,staff_assistant_email,car_care_manager,car_care_mngr_email,car_care_assistant_manager,car_care_assistant_mngr_email,region_tvl_sales_mgr,region_tvl_sales_mgr_email,retail_sales_coach,retail_sales_coach_email,xactly_retail_store_nbr,xactly_car_care_store_nbr,region,car_care_general_manager,car_care_general_mngr_email,car_care_dirctr,car_care_dirctr_email,car_care_district_nbr,car_care_cost_center_nbr,tire_cost_center_nbr,admin_cost_center_nbr,occupancy_cost_center_nbr,retail_general_manager,retail_general_mngr_email,district_manager,district_mngr_email,retail_district_nbr,retail_cost_center_nbr,tvl_cost_center_nbr,mbrs_cost_center_nbr,facility_cost_center_nbr,national_ats_nbr,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number"
	#},
    'dim_contact_segmentation|contact_segmntn_adw_key|integrated_update_datetime':
    {
     'contact_segmntn_adw_key, contact_adw_key, segment_source, segment_panel, segment_cd, segment_desc, actual_score_ind, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrated_insert_datetime, integrated_insert_batch_number, integrated_update_datetime, integrated_update_batch_number':"contact_segmntn_adw_key, contact_adw_key, segment_source, segment_panel, segment_cd, segment_desc, safe_cast(actual_score_ind as string) as actual_score_ind, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrated_insert_datetime, integrated_insert_batch_number, integrated_update_datetime, integrated_update_batch_number"
    }
    #,
    #'dim_product|product_adw_key|NULL':
    #{
    # #'product_adw_key, product_category_adw_key, vendor_adw_key, product_sku, product_sku_key, product_sku_desc, product_sku_effective_dt, product_sku_expiration_dt, product_item_nbr_cd, product_item_desc, product_vendor_nm, product_service_category_cd, product_status_cd, product_dynamic_sku, product_dynamic_sku_desc, product_dynamic_sku_key, product_unit_cost, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number': "product_adw_key, product_category_adw_key, vendor_adw_key, product_sku, product_sku_key, product_sku_desc, ifnull(safe_cast( if( product_sku_effective_dt < '1900-01-01', null, product_sku_effective_dt) as string),'null') as  product_sku_effective_dt, ifnull(safe_cast( if( product_sku_expiration_dt < '1900-01-01', null, product_sku_expiration_dt) as string),'null') as product_sku_expiration_dt, product_item_nbr_cd, product_item_desc, product_vendor_nm, product_service_category_cd, product_status_cd, product_dynamic_sku, product_dynamic_sku_desc, product_dynamic_sku_key, safe_cast(product_unit_cost as string) as product_unit_cost, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number"
    # 'product_adw_key, product_category_adw_key, vendor_adw_key, product_sku, product_sku_key, product_sku_desc, product_sku_effective_dt, product_sku_expiration_dt, product_item_nbr_cd, product_item_desc, product_vendor_nm, product_service_category_cd, product_status_cd, product_dynamic_sku, product_dynamic_sku_desc, product_dynamic_sku_key, product_unit_cost, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number': "product_adw_key, product_category_adw_key, vendor_adw_key, product_sku, product_sku_key, product_sku_desc, product_sku_effective_dt, product_sku_expiration_dt, product_item_nbr_cd, product_item_desc, product_vendor_nm, product_service_category_cd, product_status_cd, product_dynamic_sku, product_dynamic_sku_desc, product_dynamic_sku_key, safe_cast(product_unit_cost as string) as product_unit_cost, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number"
    #}
}


SOURCE_DB_NAME = 'insurance'
DAG_TITLE = 'outbound_insurance_cdl'
SCHEDULE = Variable.get("outbound_insurance_cdl_schedule", default_var='') or None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22, 0, tzinfo=local_tz),
    'email': [FAIL_EMAIL],
    'catchup': False,
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    'file_partition': iu.build_file_partition(),
    'params': {
        'dag_name': DAG_TITLE
    }
}

def parse_value(dic):
   for (k, v) in dic.items():
       column =  k
       select = v
   return column, select

def read_hwm_table(table):

    client = iu._get_bq_client()

    #query = f"""SELECT dataset_name, table_name, safe_cast(ifnull(safe_cast(highwatermark as date), DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY)) as string) as highwatermark  FROM `{int_u.INGESTION_PROJECT}.admin.ingestion_highwatermarks` where dataset_name = 'outbound' and table_name = '{ table }' """
    query = f"""SELECT dataset_name, table_name, safe_cast(ifnull(safe_cast(highwatermark as date), safe_cast('2020-01-01' as date)) as string) as highwatermark  FROM `{int_u.INGESTION_PROJECT}.admin.ingestion_highwatermarks` where dataset_name = 'outbound' and table_name = '{ table }' """

    query_job = client.query(query, location="US")

    hwm_value = '{{ execution_date.strftime("%Y-%m-%d") }}'
    a = {}
    for row in query_job:  # expects [dataset_name, table_name, highwatermark_value]
        ds, tablenm, hwm = row[:3]
        a[tablenm] = hwm
        hwm_value = a.get(table, '')
    return hwm_value

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=3, max_active_runs=1)

# grab table configuration data and highwatermark control table data
config_cluster = iu.read_webserver_cluster_config(SOURCE_DB_NAME)

create_cluster = iu.create_dataproc_cluster(config_cluster, dag)
delete_cluster = iu.delete_dataproc_cluster(config_cluster, dag)


for table_pk_hwmcol, fields in config_tables.items():

    table = table_pk_hwmcol.split('|')[0]
    pk = table_pk_hwmcol.split('|')[1]
    hwm_column = table_pk_hwmcol.split('|')[2]
    column, select = parse_value(fields)
    hwm_date = read_hwm_table (table)

    if hwm_column != "NULL":
       #create_table_sql = f"""
       #                   CREATE or REPLACE table `{ int_u.INTEGRATION_PROJECT }.adw_work.outbound_{ table }_work_source`  as select { select } from `{ int_u.INTEGRATION_PROJECT }.adw.{ table }` where actv_ind = 'Y' and safe_cast( { hwm_column } as date) <= CURRENT_DATE() and safe_cast( { hwm_column } as date) > (select ifnull(safe_cast(highwatermark as date), DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY)) from `{ int_u.INGESTION_PROJECT }.admin.ingestion_highwatermarks` where dataset_name = 'outbound' and table_name = '{table}')
       #                   """
        create_table_sql = f"""
                           CREATE or REPLACE table `{ int_u.INTEGRATION_PROJECT }.adw_work.outbound_{ table }_work_source`  as select { select } from `{ int_u.INTEGRATION_PROJECT }.adw.{ table }` where actv_ind = 'Y' and safe_cast( { hwm_column } as date) <= CURRENT_DATE() and safe_cast( { hwm_column } as date) > (select ifnull(safe_cast(highwatermark as date), safe_cast('2020-01-01' as date)) from `{ int_u.INGESTION_PROJECT }.admin.ingestion_highwatermarks` where dataset_name = 'outbound' and table_name = '{table}')
                           """

        #setupsql = f"""
        #            Delete from { table } where { hwm_column } > convert(date,'{hwm_date}')
        #            """
        # 
        #setupsql = f"""
        #               select 1
        #           """
        setupsql = f"""
                      TRUNCATE TABLE {table}
                  """
        hwm_update = f"""
           update `{int_u.INGESTION_PROJECT}.admin.ingestion_highwatermarks` set highwatermark = ( select safe_cast(ifnull(max(safe_cast({ hwm_column } as date)),CURRENT_DATE()) as string) from  `{int_u.INTEGRATION_PROJECT}.adw_work.outbound_{table}_work_source` where   (Select  max(safe_cast({ hwm_column } as date)) from  `{int_u.INTEGRATION_PROJECT}.adw_work.outbound_fact_sales_header_work_source`) <= CURRENT_DATE()  ) 
           where dataset_name = 'outbound' and table_name = '{ table }'
           """
    else:
        
        create_table_sql = f"""
                           CREATE or REPLACE table `{ int_u.INTEGRATION_PROJECT }.adw_work.outbound_{ table }_work_source`  as select { select } from `{ int_u.INTEGRATION_PROJECT }.adw.{ table }` where actv_ind ='Y'
                           """
        setupsql = f"""
                    TRUNCATE TABLE {table}
                    """
        hwm_update = f"""
                 update `{int_u.INGESTION_PROJECT}.admin.ingestion_highwatermarks` set highwatermark = safe_cast(CURRENT_DATE() as string)
                 where dataset_name = 'outbound' and table_name = '{ table }'
                 """

    prepare_work_source = BigQueryOperator(task_id=f"prepare_{table}_work_source",
                                           #sql= "CREATE or REPLACE table `{{ params.integration_project }}.adw_work.outbound_{{ params.table }}_work_source`  as select {{ params.fields }} from `{{ params.integration_project }}.adw.{{ params.table }}` where actv_ind ='Y' ",
                                           sql=create_table_sql,
                                           dag=dag,
                                           use_legacy_sql=False,
                                           retries=0,
                                           params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                                   'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                                   'table': table,
                                                   'fields': select})
    bq_2_gcs = BashOperator(task_id=f"bq_2_gcs_{table}",
                             bash_command='bq extract --project_id={{ params.integration_project }}  --field_delimiter "^" --print_header=false adw_work.outbound_{{ params.table }}_work_source gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}/{{ params.table }}_*.psv',
                             dag=dag,
                             params= {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                      'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                      'table': table,
                                      'fields': column})
    audit_start_date = BashOperator(task_id=f"audit_start_date_{table}",
                                    bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-insurance"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "update dbo.cdl_load_status  set [load_start_dtm] = CURRENT_TIMESTAMP, [load_complete_dtm] = NULL where table_name = '"'"'{{ params.table }}'"'"' "    ',
                                    dag=dag,
                                    params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                            'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                            'environment': Variable.get("environment"),
                                            'Cdl_Server': Variable.get("Cdl_Server"),
                                            'Cdl_db': Variable.get("Cdl_db"),
                                            'table': table,
                                            'fields': column})
    
    prepsql = BashOperator(task_id=f"prepsql_{table}",
                             #bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-bq"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_ingest_utilities/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_ingest_utilities/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_ingest_utilities/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName=adw_cdl;" --username="SVCGCPUSER"  --password-file="gs://adw_ingest_utilities/source_db_account_credentials/cdl/dev/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "TRUNCATE TABLE {{ params.table }}"    ',
                             bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-insurance"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "{{ params.sql }}"    ',
                             dag=dag,
                             params = {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                       'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                       'environment': Variable.get("environment"),
                                       'Cdl_Server':Variable.get("Cdl_Server"),
                                       'Cdl_db': Variable.get("Cdl_db"),
                                       'table': table,
                                       'fields': column,
                                       'sql': setupsql})
    gcs_2_sql = BashOperator(task_id=f"gcs_2_sql_{table}",
                             #bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}  --cluster="sqoop-gcp-outbound-bq"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_ingest_utilities/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_ingest_utilities/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_ingest_utilities/ingestion_jars/jtds-1.3.1.jar"  -- export -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName=adw_cdl;" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --table "{{ params.table }}" --export-dir "gs://adw-dev-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}" -m 1  --update-key=" '+pk+' " --update-mode=allowinsert  -input-fields-terminated-by "|" --columns  "{{ params.fields }}" ',
                             #bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}  --cluster="sqoop-gcp-outbound-insurance"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- export -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --table "{{ params.table }}" --export-dir "gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}" -m 1  --update-key=" ' + pk + ' " --update-mode=allowinsert  -input-fields-terminated-by "^" --columns  "{{ params.fields }}" ',
                             bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}  --cluster="sqoop-gcp-outbound-insurance"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- export -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --table "{{ params.table }}" --export-dir "gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}" -m 1  -input-fields-terminated-by "^" --columns  "{{ params.fields }}" ',
                             dag=dag,
                             params= {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                       'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                       'environment': Variable.get("environment"),
                                       'Cdl_Server':Variable.get("Cdl_Server"),
                                       'Cdl_db': Variable.get("Cdl_db"),
                                       'table': table,
                                       'fields': column})
    audit_complete_date = BashOperator(task_id=f"audit_complete_date_{table}",
                                       bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-insurance"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "update dbo.cdl_load_status  set [load_complete_dtm] = CURRENT_TIMESTAMP where table_name = '"'"'{{ params.table }}'"'"' "    ',
                                       dag=dag,
                                       params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                               'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                               'environment': Variable.get("environment"),
                                               'Cdl_Server': Variable.get("Cdl_Server"),
                                               'Cdl_db': Variable.get("Cdl_db"),
                                               'table': table,
                                               'fields': column})
    HWMUpdate = BigQueryOperator(task_id=f"HWMUpdate_{table}",
                                sql=hwm_update,
                                use_legacy_sql=False,
                                start_date=datetime(2019, 8, 22),
                                retries=0)
    
    create_cluster >> prepare_work_source >> bq_2_gcs >> audit_start_date >> prepsql >> gcs_2_sql >> audit_complete_date >> HWMUpdate >> delete_cluster

    # if hwm_column != "NULL":
    #     create_cluster >> prepare_work_source >> bq_2_gcs >> audit_start_date  >> gcs_2_sql >> audit_complete_date >> HWMUpdate >> delete_cluster
    # else:
    #     create_cluster >> prepare_work_source >> bq_2_gcs >> audit_start_date >> truncate_table >> gcs_2_sql >> audit_complete_date >> HWMUpdate >> delete_cluster

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([delete_cluster])