from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 17),
    'email': ['bill@bill.com'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

with DAG('UTIL-RESET-MEMBERSHIP', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    dim_member = BigQueryOperator(
        task_id='dim_member',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_member` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_member_auto_renewal_card = BigQueryOperator(
        task_id='dim_member_auto_renewal_card',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_member_auto_renewal_card` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_member_rider = BigQueryOperator(
        task_id='dim_member_rider',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_member_rider` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership = BigQueryOperator(
        task_id='dim_membership',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_membership` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership_fee = BigQueryOperator(
        task_id='dim_membership_fee',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_membership_fee` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership_marketing_segmentation = BigQueryOperator(
        task_id='dim_membership_marketing_segmentation',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_membership_marketing_segmentation` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership_payment_plan = BigQueryOperator(
        task_id='dim_membership_payment_plan',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_membership_payment_plan` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership_solicitation = BigQueryOperator(
        task_id='dim_membership_solicitation',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.dim_membership_solicitation` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_billing_detail = BigQueryOperator(
        task_id='membership_billing_detail',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.membership_billing_detail` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_billing_summary = BigQueryOperator(
        task_id='membership_billing_summary',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.membership_billing_summary` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_gl_payments_applied = BigQueryOperator(
        task_id='membership_gl_payments_applied',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.membership_gl_payments_applied` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_payment_applied_detail = BigQueryOperator(
        task_id='membership_payment_applied_detail',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.membership_payment_applied_detail` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_payment_applied_summary = BigQueryOperator(
        task_id='membership_payment_applied_summary',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.membership_payment_applied_summary` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_payments_received = BigQueryOperator(
        task_id='membership_payments_received',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.membership_payments_received` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    sales_agent_activity = BigQueryOperator(
        task_id='sales_agent_activity',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.member.sales_agent_activity` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_aca_office = BigQueryOperator(
        task_id='dim_aca_office',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_comment = BigQueryOperator(
        task_id='dim_comment',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_comment` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_employee_role = BigQueryOperator(
        task_id='dim_employee_role',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_employee = BigQueryOperator(
        task_id='dim_employee',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')