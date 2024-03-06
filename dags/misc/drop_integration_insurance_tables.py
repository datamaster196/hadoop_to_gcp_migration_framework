#######################################################################################################################
##  DAG_NAME :  UTIL-DROP-INTEGRATION-INSURANCE-TABLES
##  PURPOSE :   DROP tables for insurance model
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

DAG_TITLE = "UTIL-DROP-INTEGRATION-INSURANCE-TABLES"

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


with DAG('UTIL-DROP-INTEGRATION-INSURANCE-TABLES', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    insurance_customer_role = BigQueryOperator(
        task_id='insurance_customer_role',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    insurance_policy = BigQueryOperator(
        task_id='insurance_policy',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    insurance_policy_line = BigQueryOperator(
        task_id='insurance_policy_line',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_state = BigQueryOperator(
        task_id='dim_state',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_state` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([insurance_customer_role,insurance_policy,insurance_policy_line,dim_state])