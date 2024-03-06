#######################################################################################################################
##  DAG_NAME :  UTIL-DROP-INTEGRATION-CONTACT-TABLES
##  PURPOSE :   DROP tables for contact info model
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


DAG_TITLE = "UTIL-DROP-INTEGRATION-CONTACT-TABLES"
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

with DAG('UTIL-DROP-INTEGRATION-CONTACT-TABLES', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    dim_address = BigQueryOperator(
        task_id='dim_address',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_email = BigQueryOperator(
        task_id='dim_email',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_name = BigQueryOperator(
        task_id='dim_name',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_phone = BigQueryOperator(
        task_id='dim_phone',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_contact_source_key = BigQueryOperator(
        task_id='dim_contact_source_key',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_address = BigQueryOperator(
        task_id='xref_contact_address',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_email = BigQueryOperator(
        task_id='xref_contact_email',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_email` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_name = BigQueryOperator(
        task_id='xref_contact_name',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    xref_contact_phone = BigQueryOperator(
        task_id='xref_contact_phone',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_phone` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    audit_contact_match = BigQueryOperator(
        task_id='audit_contact_match',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw.audit_contact_match` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_contact_info = BigQueryOperator(
        task_id='dim_contact_info',
        bql="DROP TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` ",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([dim_address, dim_email, dim_name, dim_phone, dim_contact_source_key, xref_contact_address, xref_contact_email, xref_contact_name, xref_contact_phone, audit_contact_match, dim_contact_info])