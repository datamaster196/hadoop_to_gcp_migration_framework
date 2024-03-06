#######################################################################################################################
##  DAG_NAME :  integrated_car_care_hist
##  PURPOSE :   Executes historical load for car care (VAST) model
##  PREREQUISITE DAGs : ingest_vast
##                      integrated_membership_mzp_hist
##                      integrated_retail_hist
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from airflow import models

from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None
SCHEDULE = Variable.get("integrated_car_care_schedule", default_var='') or None


DAG_TITLE = "integrated_car_care_hist"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 11),
    'email': [FAIL_EMAIL],
    'email_on_success': True,
    'email_on_failure': True, 
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=60),
    'params': {
        'dag_name': DAG_TITLE
    }
}

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None, max_active_runs=1 ,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
    ######################################################################
    # TASKS
 
    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_dim_name', "dml/integration/incremental/dim_name/vast/dim_name_vast.sql")

    ## phone
    task_phone_insert = int_u.run_query('insert_dim_phone', "dml/integration/incremental/dim_phone/vast/dim_phone_vast.sql")

    ## address
    task_address_insert = int_u.run_query('insert_dim_address', "dml/integration/incremental/dim_address/vast/dim_address_vast.sql")

    ## email
    task_email_insert = int_u.run_query('insert_dim_email', "dml/integration/incremental/dim_email/vast/dim_email_vast.sql")
	
    # Contact Matching Logic 
    task_contact_work_source = int_u.run_query("build_contact_work_source","dml/integration/historical/contact_info/vast/contact_vast_work_source.sql")
    task_contact_work_target = int_u.run_query("build_contact_work_target", "dml/integration/incremental/contact_info/vast/contact_vast_work_target.sql")
    task_contact_work_stage = int_u.run_query("build_contact_work_matched", "dml/integration/incremental/contact_info/vast/contact_vast_work_matching.sql")


    # Contact XREF's
    ## name
    task_xref_contact_name_merge = int_u.run_query('merge_name', "dml/integration/incremental/contact_info/vast/xref_contact_name_vast_merge.sql")

    ## address
    task_xref_contact_address_merge = int_u.run_query('merge_address', "dml/integration/incremental/contact_info/vast/xref_contact_address_vast_merge.sql")

    ## phone
    task_xref_contact_phone_merge = int_u.run_query('merge_phone', "dml/integration/incremental/contact_info/vast/xref_contact_phone_vast_merge.sql")

    ## email
    task_xref_contact_email_merge = int_u.run_query('merge_email', "dml/integration/incremental/contact_info/vast/xref_contact_email_vast_merge.sql")


    ## keys
    task_dim_contact_source_key_merge = int_u.run_query('merge_keys', "dml/integration/incremental/contact_info/vast/dim_contact_source_key_vast_merge.sql")

    # contact
    task_contact_insert = int_u.run_query('insert_contact', "dml/integration/incremental/contact_info/vast/dim_contact_info_vast_insert.sql")
    task_contact_merge = int_u.run_query('merge_contact', "dml/integration/incremental/contact_info/shared/dim_contact_info_merge.sql")
    task_contact_match_audit = int_u.run_query('match_audit', "dml/integration/incremental/contact_info/vast/audit_contact_match_vast_insert.sql")
    
    # fact_sales_header_car_care  
    task_fact_sales_header_car_care_prep_work = int_u.run_query('fact_sales_header_car_care_prep_work',
                                         "dml/integration/historical/fact_sales_header/car_care/fact_sales_header_car_care_prep_work.sql")
  
    task_fact_sales_header_car_care_merge = int_u.run_query('fact_sales_header_merge',
                                         "dml/integration/historical/fact_sales_header/car_care/fact_sales_header_car_care.sql")
    

AllTaskSuccess = EmailOperator(
       dag=dag,
       trigger_rule=TriggerRule.ALL_SUCCESS,
       task_id="AllTaskSuccess",
       to=SUCCESS_EMAIL,
       subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
       html_content='<h3>All Task completed successfully" </h3>')
    ######################################################################
    # DEPENDENCIES

task_name_insert >> task_contact_work_source
task_phone_insert >> task_contact_work_source
task_address_insert >> task_contact_work_source
task_email_insert >> task_contact_work_source
    
task_contact_work_source >> task_contact_work_stage
task_contact_work_target >> task_contact_work_stage

task_contact_work_stage >> task_contact_insert

task_contact_insert >> task_xref_contact_name_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_address_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_phone_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_email_merge >> task_contact_merge
task_contact_insert >> task_dim_contact_source_key_merge >> task_contact_merge

task_contact_merge >> task_contact_match_audit 
   
# fact_sales_header
task_contact_match_audit >> task_fact_sales_header_car_care_prep_work >> task_fact_sales_header_car_care_merge



AllTaskSuccess.set_upstream([task_fact_sales_header_car_care_merge])
