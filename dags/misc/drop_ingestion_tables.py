#######################################################################################################################
##  DAG_NAME :  UTIL-DROP-INGESTION-TABLES
##  PURPOSE :   DROP tables in ingestion layer
##  PREREQUISITE DAGs :
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from airflow import models
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "UTIL-DROP-INGESTION-TABLES"

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



with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:


   ######################################################################
    # TASKS

    # drop tables
    task_drop_ingest_tables = int_u.run_query('drop_ingest_tables',
                                          "dbcleanup/drop_ingestion_tables.sql")



    ######################################################################
    # DEPENDENCIES
# Drop Ingest tables
task_drop_ingest_tables

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([task_drop_ingest_tables])