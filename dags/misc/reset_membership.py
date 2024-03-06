#######################################################################################################################
##  DAG_NAME :  UTIL-RESET-MEMBERSHIP
##  PURPOSE :   DAG to delete data for membership model
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

DAG_TITLE = "UTIL-RESET-MEMBERSHIP"

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

# Define Delete SQL Queries
#delete_dim_employee = f"""
#DELETE FROM `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw_pii.dim_employee` a
#WHERE exists (select b.emp_adw_key from `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_employee_role` b
#             where a.emp_adw_key = b.emp_adw_key and b.emp_biz_role_line_cd = 'Membership')
#"""

#delete_dim_product = f"""
#DELETE FROM `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_product` a
#where exists ( select  b.product_category_adw_key from `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_product_category` b
#  inner join `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_business_line` c on c.biz_line_adw_key = b.biz_line_adw_key
#  where a.product_category_adw_key = b.product_category_adw_key
#  and c.biz_line_desc = 'Membership')
#"""


with DAG('UTIL-RESET-MEMBERSHIP', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    dim_member = BigQueryOperator(
        task_id='dim_member',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_member_auto_renewal_card = BigQueryOperator(
        task_id='dim_member_auto_renewal_card',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_member_rider = BigQueryOperator(
        task_id='dim_member_rider',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership = BigQueryOperator(
        task_id='dim_membership',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership_fee = BigQueryOperator(
        task_id='dim_membership_fee',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_mbrs_marketing_segmntn = BigQueryOperator(
        task_id='dim_mbrs_marketing_segmntn',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_mbrs_marketing_segmntn` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership_payment_plan = BigQueryOperator(
        task_id='dim_membership_payment_plan',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    dim_membership_solicitation = BigQueryOperator(
        task_id='dim_membership_solicitation',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_billing_detail = BigQueryOperator(
        task_id='membership_billing_detail',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_billing_summary = BigQueryOperator(
        task_id='membership_billing_summary',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    mbrs_gl_payments_applied = BigQueryOperator(
        task_id='mbrs_gl_payments_applied',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    mbrs_payment_applied_detail = BigQueryOperator(
        task_id='mbrs_payment_applied_detail',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    mbrs_payment_applied_summary = BigQueryOperator(
        task_id='mbrs_payment_applied_summary',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    membership_payment_received = BigQueryOperator(
        task_id='membership_payment_received',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    sales_agent_activity = BigQueryOperator(
        task_id='sales_agent_activity',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity` WHERE 1=1",
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
    #dim_employee_role = BigQueryOperator(
    #    task_id='dim_employee_role',
    #    bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role` WHERE emp_biz_role_line_cd = 'Membership'",
    #    use_legacy_sql=False,
    #    write_disposition='WRITE_TRUNCATE')
    #dim_employee = BigQueryOperator(
    #    task_id='dim_employee',
    #    bql=delete_dim_employee,
    #    use_legacy_sql=False,
    #    write_disposition='WRITE_TRUNCATE')
    #dim_product = BigQueryOperator(
    #    task_id='dim_product',
    #    bql=delete_dim_product,
    #    use_legacy_sql=False,
    #    write_disposition='WRITE_TRUNCATE')

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

#AllTaskSuccess.set_upstream([dim_member,dim_member_auto_renewal_card,dim_member_rider,dim_membership,dim_membership_fee,dim_mbrs_marketing_segmntn,dim_membership_payment_plan,dim_membership_solicitation,membership_billing_detail,membership_billing_summary,mbrs_gl_payments_applied,mbrs_payment_applied_detail,mbrs_payment_applied_summary,membership_payment_received,sales_agent_activity,dim_aca_office,dim_comment,dim_employee_role,dim_employee,dim_product])
AllTaskSuccess.set_upstream([dim_member,dim_member_auto_renewal_card,dim_member_rider,dim_membership,dim_membership_fee,dim_mbrs_marketing_segmntn,dim_membership_payment_plan,dim_membership_solicitation,membership_billing_detail,membership_billing_summary,mbrs_gl_payments_applied,mbrs_payment_applied_detail,mbrs_payment_applied_summary,membership_payment_received,sales_agent_activity,dim_aca_office,dim_comment])