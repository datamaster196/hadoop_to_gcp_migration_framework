#######################################################################################################################
##  DAG_NAME :  UTIL-RESET-DEMOGRAPHICS
##  PURPOSE :   DAG to delete data for demographics model
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
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "UTIL-RESET-DEMOGRAPHICS"

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

with DAG('UTIL-RESET-DEMOGRAPHICS', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    dim_demogr_person = BigQueryOperator(
        task_id='dim_demo_person',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_demogr_insurance_score = BigQueryOperator(
        task_id='dim_demo_ins_score',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_ins_score` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([dim_demogr_person, dim_demogr_insurance_score])