#######################################################################################################################
##  DAG_NAME :  integrated_marketing_hist
##  PURPOSE :   Executes historical load for marketing model
##  PREREQUISITE DAGs : ingest_eds
##                      ingest_mzp
##                      ingest_sfmc
##                      ingest_OptOut_Data_single_file
##                      integrated_membership_mzp_hist
##                      integrated_insurance_hist
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

DAG_TITLE = "integrated_marketing_hist"

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
    'execution_timeout': timedelta(minutes=60),
    'params': {
        'dag_name': DAG_TITLE
    }
}

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None , max_active_runs=1,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
    ######################################################################
    # TASKS
 
    # sfmc_fact_contact_event_optout  
    task_sfmc_source = int_u.run_query('sfmc_fact_contact_event_optout_source',"dml/integration/historical/fact_contact_event_optout/sfmc_fact_contact_event_optout/sfmc_fact_contact_event_optout_work_source.sql")
    task_sfmc_transformed = int_u.run_query('sfmc_fact_contact_event_optout_transformed',"dml/integration/historical/fact_contact_event_optout/sfmc_fact_contact_event_optout/sfmc_fact_contact_event_optout_work_transform.sql")
    task_sfmc_insert = int_u.run_query('sfmc_fact_contact_event_optout_insert', "dml/integration/historical/fact_contact_event_optout/sfmc_fact_contact_event_optout/sfmc_fact_contact_event_optout.sql")

    
    # member_fact_contact_event_optout  
    task_member_source = int_u.run_query('member_fact_contact_event_optout_source',"dml/integration//historical/fact_contact_event_optout/member_fact_contact_event_optout/member_fact_contact_event_optout_work_source.sql")
    task_member_transformed = int_u.run_query('member_fact_contact_event_optout_transformed',"dml/integration/historical/fact_contact_event_optout/member_fact_contact_event_optout/member_fact_contact_event_optout_work_transform.sql")
    task_member_insert = int_u.run_query('member_fact_contact_event_optout_insert', "dml/integration/historical/fact_contact_event_optout/member_fact_contact_event_optout/member_fact_contact_event_optout.sql")

    # aaacom_fact_contact_event_optout 
    task_aaacom_source = int_u.run_query('aaacom_fact_contact_event_optout_source',
                                         "dml/integration//historical/fact_contact_event_optout/aaacom_fact_contact_event_optout/aaacom_fact_contact_event_optout_work_source.sql")
    task_aaacom_transformed = int_u.run_query('aaacom_fact_contact_event_optout_transformed',
                                              "dml/integration/historical/fact_contact_event_optout/aaacom_fact_contact_event_optout/aaacom_fact_contact_event_optout_work_transform.sql")
    task_aaacom_insert = int_u.run_query('aaacom_fact_contact_event_optout_insert',
                                         "dml/integration/historical/fact_contact_event_optout/aaacom_fact_contact_event_optout/aaacom_fact_contact_event_optout.sql")

    # dim_contact_optout
    task_contact_optout_pre_source = int_u.run_query('contact_optout_pre_source',
                                          "dml/integration/historical/dim_contact_optout/contact_optout_work_pre_source.sql")
    
    task_contact_optout_source = int_u.run_query('contact_optout_source',
                                          "dml/integration/historical/dim_contact_optout/contact_optout_work_source.sql")
    task_contact_optout_transformed = int_u.run_query('contact_optout_transformed',
                                               "dml/integration/historical/dim_contact_optout/contact_optout_work_transformed.sql")
    task_contact_optout_typ_2_hist = int_u.run_query('contact_optout_work_typ_2_hist',
                                         "dml/integration/historical/dim_contact_optout/contact_optout_work_type_2_hist.sql")
    task_contact_optout_insert = int_u.run_query('contact_optout_insert', "dml/integration/historical/dim_contact_optout/dim_contact_optout.sql")

    # DEPENDENCIES


AllTaskSuccess = EmailOperator(
       dag=dag,
       trigger_rule=TriggerRule.ALL_SUCCESS,
       task_id="AllTaskSuccess",
       to=SUCCESS_EMAIL,
       subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
       html_content='<h3>All Task completed successfully" </h3>')

    ######################################################################    
    # DEPENDENCIES

    # sfmc_fact_contact_event_optout
task_sfmc_source >> task_sfmc_transformed >> task_sfmc_insert
task_aaacom_source >> task_aaacom_transformed
task_member_source >> task_member_transformed
[task_sfmc_insert,task_aaacom_transformed] >> task_aaacom_insert
[task_aaacom_insert,task_member_transformed] >> task_member_insert
    
# dim_contact_optout
[task_member_insert] >> task_contact_optout_pre_source >> task_contact_optout_source  >> task_contact_optout_transformed >> task_contact_optout_typ_2_hist >> task_contact_optout_insert
AllTaskSuccess.set_upstream([task_contact_optout_insert])
