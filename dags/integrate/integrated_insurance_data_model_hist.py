#######################################################################################################################
##  DAG_NAME :  integrated_insurance_hist
##  PURPOSE :   Executes historical load for insurance model
##  PREREQUISITE DAGs : ingest_epic
##                      integrated_membership_mzp_hist
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from airflow import models

from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "integrated_insurance_hist"

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

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None, max_active_runs=1 ,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
    ######################################################################
    # TASKS

    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_dim_name', "dml/integration/incremental/dim_name/epic/dim_name_epic.sql")

    ## phone
    task_phone_insert = int_u.run_query('insert_dim_phone', "dml/integration/incremental/dim_phone/epic/dim_phone_epic.sql")

    ## address
    task_address_insert = int_u.run_query('insert_dim_address', "dml/integration/incremental/dim_address/epic/dim_address_epic.sql")

    ## email
    task_email_insert = int_u.run_query('insert_dim_email', "dml/integration/incremental/dim_email/epic/dim_email_epic.sql")
	
    # Contact Matching Logic 
    task_contact_work_source = int_u.run_query("build_contact_work_source","dml/integration/historical/contact_info/epic/contact_epic_work_source.sql")
    task_contact_work_target = int_u.run_query("build_contact_work_target", "dml/integration/incremental/contact_info/epic/contact_epic_work_target.sql")
    task_contact_work_stage = int_u.run_query("build_contact_work_matched", "dml/integration/incremental/contact_info/epic/contact_epic_work_matching.sql")


    # Contact XREF's
    ## name
    task_xref_contact_name_merge = int_u.run_query('merge_name', "dml/integration/incremental/contact_info/epic/xref_contact_name_epic_merge.sql")

    ## address
    task_xref_contact_address_merge = int_u.run_query('merge_address', "dml/integration/incremental/contact_info/epic/xref_contact_address_epic_merge.sql")

    ## phone
    task_xref_contact_phone_merge = int_u.run_query('merge_phone', "dml/integration/incremental/contact_info/epic/xref_contact_phone_epic_merge.sql")

    ## email
    task_xref_contact_email_merge = int_u.run_query('merge_email', "dml/integration/incremental/contact_info/epic/xref_contact_email_epic_merge.sql")


    ## keys
    task_dim_contact_source_key_merge = int_u.run_query('merge_keys', "dml/integration/incremental/contact_info/epic/dim_contact_source_key_epic_merge.sql")

    # contact
    task_contact_insert = int_u.run_query('insert_contact', "dml/integration/incremental/contact_info/epic/dim_contact_info_epic_insert.sql")
    task_contact_merge = int_u.run_query('merge_contact', "dml/integration/incremental/contact_info/shared/dim_contact_info_merge.sql")
    task_contact_match_audit = int_u.run_query('match_audit', "dml/integration/incremental/contact_info/epic/audit_contact_match_epic_insert.sql")

    # Dim_Vendor
    task_vendor_source = int_u.run_query('vendor_source', "dml/integration/historical/dim_vendor/insurance_vendor_work_source.sql")
    task_vendor_transformed = int_u.run_query('vendor_transformed', "dml/integration/historical/dim_vendor/insurance_vendor_work_transformed.sql")
    task_vendor_stage = int_u.run_query('vendor_stage', "dml/integration/historical/dim_vendor/insurance_vendor_work_type_2_hist.sql")
    task_vendor_insert = int_u.run_query('vendor_insert', "dml/integration/historical/dim_vendor/insurance_dim_vendor.sql")

    # Employee -Workarea Loads
    task_employee_work_source = int_u.run_query("insurance_employee_work_source", "dml/integration/historical/dim_employee/insurance_dim_employee_work_source.sql")
    task_employee_work_transformed = int_u.run_query("insurance_employee_work_transformed", "dml/integration/historical/dim_employee/insurance_dim_employee_work_transformed.sql")
    task_employee_insert_email = int_u.run_query('insurance_employee_insert_email', "dml/integration/historical/dim_employee/insurance_dim_employee_insert_email.sql")
    task_employee_insert_name = int_u.run_query('insurance_employee_insert_name',  "dml/integration/historical/dim_employee/insurance_dim_employee_insert_name.sql")
    task_employee_insert_phone = int_u.run_query('insurance_employee_insert_phone',  "dml/integration/historical/dim_employee/insurance_dim_employee_insert_phone.sql")
    task_employee_work_final_staging = int_u.run_query("insurance_employee_work_stage", "dml/integration/historical/dim_employee/insurance_dim_employee_work_stage.sql")
    task_employee_insert = int_u.run_query('insurance_insert_dim_employee', "dml/integration/historical/dim_employee/insurance_dim_employee.sql")


    # Vendor - Workarea loads dml/integration/historical/dim_vendor/

    # Dim_Product
    task_product_source = int_u.run_query('product_source', "dml/integration/historical/dim_product/insurance_dim_product_work_source.sql")
    task_product_transformed = int_u.run_query('product_transformed', "dml/integration/historical/dim_product/insurance_dim_product_work_transformed.sql")
    task_product_stage = int_u.run_query('product_stage', "dml/integration/historical/dim_product/insurance_dim_product_work_type_2_hist.sql")
    task_product_insert = int_u.run_query('product_insert', "dml/integration/historical/dim_product/insurance_dim_product.sql")


    # product - Workarea loads dml/integration/historical/dim_product/
    
    # Insurance_Policy
    task_insurance_policy_source = int_u.run_query('policy_source',"dml/integration/historical/insurance_policy/insurance_policy_work_source.sql")
    task_insurance_policy_transformed = int_u.run_query('policy_transformed',"dml/integration/historical/insurance_policy/insurance_policy_work_transformed.sql")
    task_insurance_policy_stage = int_u.run_query('policy_stage',"dml/integration/historical/insurance_policy/insurance_policy_type_2_hist.sql")
    task_insurance_policy_insert = int_u.run_query('policy_insert',"dml/integration/historical/insurance_policy/insurance_policy.sql")
    # Insurance_Policy_Line
    task_insurance_policy_line_source = int_u.run_query('policy_line_source',"dml/integration/historical/insurance_policy_line/insurance_policy_line_work_source.sql")
    task_insurance_policy_line_transformed = int_u.run_query('policy_line_transformed',"dml/integration/historical/insurance_policy_line/insurance_policy_line_work_transformed.sql")
    task_insurance_policy_line_stage = int_u.run_query('policy_line_stage',"dml/integration/historical/insurance_policy_line/insurance_policy_line_work_type_2_hist.sql")
    task_insurance_policy_line_insert = int_u.run_query('policy_line_insert',"dml/integration/historical/insurance_policy_line/insurance_policy_line.sql")
    # Insurance_Customer_Role
    task_insurance_customer_role_source = int_u.run_query('insurance_customer_role_source',"dml/integration/historical/insurance_customer_role/insurance_customer_role_work_source.sql")
    task_insurance_customer_role_transformed = int_u.run_query('insurance_customer_role_transformed',"dml/integration/historical/insurance_customer_role/insurance_customer_role_work_transformed.sql")
    task_insurance_customer_role_stage = int_u.run_query('insurance_customer_role_stage',"dml/integration/historical/insurance_customer_role/insurance_customer_role_work_type_2_hist.sql")
    task_insurance_customer_role_insert = int_u.run_query('insurance_customer_role_insert', "dml/integration/historical/insurance_customer_role/insurance_customer_role.sql")
    
    
    
    
    
    # DEPENDENCIES

# contact
task_name_insert >> task_contact_work_source
task_phone_insert >> task_contact_work_source
task_address_insert >> task_contact_work_source
task_email_insert >> task_contact_work_source
    
task_contact_work_source >> task_contact_work_stage
task_contact_work_target >> task_contact_work_stage

task_contact_work_stage >> task_contact_insert

task_contact_insert >> task_xref_contact_name_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_address_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_phone_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_email_merge >> task_contact_merge
task_contact_insert >> task_dim_contact_source_key_merge >> task_contact_merge

task_contact_merge >> task_contact_match_audit
    
# dim_vendor
task_address_insert >> task_vendor_source >> task_vendor_transformed >> task_vendor_stage >> task_vendor_insert
# dim_product
task_vendor_insert >> task_product_source >> task_product_transformed >>  task_product_stage >> task_product_insert

# employee
task_employee_work_source >> task_employee_work_transformed >> [task_employee_insert_email,
                                                                task_employee_insert_name , task_employee_insert_phone] >> task_employee_work_final_staging >> task_employee_insert

# Insurance_Policy 
task_insurance_policy_source >> task_insurance_policy_transformed >> task_insurance_policy_stage >> task_insurance_policy_insert


# Insurance_Policy_Line
[task_product_insert,task_insurance_policy_insert] >> task_insurance_policy_line_source >> task_insurance_policy_line_transformed >> task_insurance_policy_line_stage >> task_insurance_policy_line_insert

# Insurance_Customer_Role
[task_insurance_policy_insert,task_contact_match_audit] >> task_insurance_customer_role_source >> task_insurance_customer_role_transformed >> task_insurance_customer_role_stage >> task_insurance_customer_role_insert
AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([task_employee_insert,task_insurance_policy_line_insert,task_insurance_customer_role_insert])


