#######################################################################################################################
##  DAG_NAME :  UTIL-ADHOC-SQL
##  PURPOSE :   DAG to execute adhoc sql
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
DAG_TITLE = "UTIL-ADHOC-SQL"

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

adhoc_sql = Variable.get("adhoc_sql", default_var='') or None

with DAG(DAG_TITLE, schedule_interval=None, catchup=False, default_args=default_args,template_searchpath='/home/airflow/gcs/dags/sql/') as dag:

      adhoc_sql = BigQueryOperator(
      task_id='adhoc_sql',
      sql=adhoc_sql,
      use_legacy_sql=False,
      write_disposition='WRITE_TRUNCATE')

AllTaskSuccess = EmailOperator(
   dag=dag,
   trigger_rule=TriggerRule.ALL_SUCCESS,
   task_id="AllTaskSuccess",
   to=SUCCESS_EMAIL,
   subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
   html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([adhoc_sql])