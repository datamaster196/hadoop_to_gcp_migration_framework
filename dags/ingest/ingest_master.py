#######################################################################################################################
##  DAG_NAME :  ingest_master
##  PURPOSE :   DAG to load data for admin layer
##  PREREQUISITE DAGs :
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from utils import ingestion_utilities as iu
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = 'ingest_master'
SCHEDULE = Variable.get("master_ingest_schedule", default_var='') or None

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

query_close_active_batch = """
UPDATE admin.ingestion_batch
SET batch_status = 'inactive', batch_end_datetime = FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', current_timestamp())
WHERE batch_status = 'active'
"""

query_start_active_batch = """
INSERT INTO admin.ingestion_batch (select coalesce(max(batch_number), 0) + 1, 'active', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', current_timestamp()), NULL from admin.ingestion_batch)"""

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, max_active_runs=1)

close_batch = BashOperator(
    task_id='close_batch', dag=dag,
    bash_command=f"""bq query --project_id={iu.INGESTION_PROJECT} \
    --use_legacy_sql=false "{query_close_active_batch}" """)

start_batch = BashOperator(
    task_id='start_batch', dag=dag,
    bash_command=f"""bq query --project_id={iu.INGESTION_PROJECT} \
    --use_legacy_sql=false "{query_start_active_batch}" """)

close_batch >> start_batch

AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([start_batch])