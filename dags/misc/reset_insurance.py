#######################################################################################################################
##  DAG_NAME :  UTIL-RESET-INSURANCE
##  PURPOSE :   DAG to delete data for insurance model
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

DAG_TITLE = "UTIL-RESET-INSURANCE"

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
# delete_dim_employee = f"""
# DELETE FROM `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw_pii.dim_employee` a
# WHERE exists (select b.emp_adw_key from `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_employee_role` b
#              where a.emp_adw_key = b.emp_adw_key and b.emp_biz_role_line_cd = 'Insurance')
# """

# delete_dim_product = f"""
# DELETE FROM `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_product` a
# where exists ( select  b.product_category_adw_key from `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_product_category` b
#   inner join `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_business_line` c on c.biz_line_adw_key = b.biz_line_adw_key
#   where a.product_category_adw_key = b.product_category_adw_key
#   and c.biz_line_desc = 'Insurance')
# """

# delete_dim_vendor = f"""
# DELETE FROM `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_vendor` a
# where exists (
#   select b.biz_line_adw_key from  `{{{{ var.value.INTEGRATION_PROJECT }}}}.adw.dim_business_line` b
#   where a.biz_line_adw_key = b.biz_line_adw_key
#   and b.biz_line_desc = 'Insurance')
# """

with DAG('UTIL-RESET-INSURANCE', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    insurance_customer_role = BigQueryOperator(
        task_id='insurance_customer_role',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    insurance_policy = BigQueryOperator(
        task_id='insurance_policy',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
    insurance_policy_line = BigQueryOperator(
        task_id='insurance_policy_line',
        bql="DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line` WHERE 1=1",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE')
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
    #dim_vendor = BigQueryOperator(
    #    task_id='dim_vendor',
    #    bql=delete_dim_vendor,
    #    use_legacy_sql=False,
    #    write_disposition='WRITE_TRUNCATE')

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

#AllTaskSuccess.set_upstream([insurance_customer_role,insurance_policy,insurance_policy_line, dim_employee, dim_product, dim_vendor])
AllTaskSuccess.set_upstream([insurance_customer_role,insurance_policy,insurance_policy_line])