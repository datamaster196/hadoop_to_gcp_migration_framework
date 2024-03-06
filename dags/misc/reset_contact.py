#######################################################################################################################
##  DAG_NAME :  UTIL-RESET-CONTACT-HIST
##  PURPOSE :   DAG to delete data for contact info model
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

DAG_TITLE = "UTIL-RESET-CONTACT-HIST"

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

with DAG('UTIL-RESET-CONTACT-HIST', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    dim_address = BigQueryOperator(
        task_id='dim_address',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_email = BigQueryOperator(
        task_id='dim_email',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_name = BigQueryOperator(
        task_id='dim_name',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_phone = BigQueryOperator(
        task_id='dim_phone',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_contact_source_key = BigQueryOperator(
        task_id='dim_contact_source_key',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_address = BigQueryOperator(
        task_id='xref_contact_address',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_email = BigQueryOperator(
        task_id='xref_contact_email',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_email` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_name = BigQueryOperator(
        task_id='xref_contact_name',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_phone = BigQueryOperator(
        task_id='xref_contact_phone',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_phone` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    audit_contact_match = BigQueryOperator(
        task_id='audit_contact_match',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.audit_contact_match` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_contact_info = BigQueryOperator(
        task_id='dim_contact_info',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([dim_address,dim_email,dim_name,dim_phone,dim_contact_source_key,xref_contact_address,xref_contact_email,xref_contact_name,xref_contact_phone,audit_contact_match,dim_contact_info])