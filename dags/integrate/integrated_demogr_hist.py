#######################################################################################################################
##  DAG_NAME :  integrated_demogr_hist
##  PURPOSE :   Executes historical load for demographic model
##  PREREQUISITE DAGs : ingest_demogra
##                      integrated_membership_mzp_hist
##                      integrated_insurance_hist
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "integrated_demogr_hist"

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

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None,template_searchpath='/home/airflow/gcs/dags/sql/', max_active_runs=1)  as dag:
    ######################################################################
    # TASKS
 
    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_dim_name', "dml/integration/incremental/dim_name/market_magnifier/dim_name_market_magnifier.sql")

    ## phone
    task_phone_insert = int_u.run_query('insert_dim_phone', "dml/integration/incremental/dim_phone/market_magnifier/dim_phone_market_magnifier.sql")

    ## address
    task_address_insert = int_u.run_query('insert_dim_address', "dml/integration/incremental/dim_address/market_magnifier/dim_address_market_magnifier.sql")

	
    # Contact Matching Logic 
    task_contact_work_source = int_u.run_query("build_contact_work_source","dml/integration/historical/contact_info/market_magnifier/contact_market_magnifier_work_source.sql")
    task_contact_work_target = int_u.run_query("build_contact_work_target", "dml/integration/incremental/contact_info/market_magnifier/contact_market_magnifier_work_target.sql")
    task_contact_work_stage = int_u.run_query("build_contact_work_matched", "dml/integration/incremental/contact_info/market_magnifier/contact_market_magnifier_work_matching.sql")


    # Contact XREF's
    ## name
    task_xref_contact_name_merge = int_u.run_query('merge_name', "dml/integration/incremental/contact_info/market_magnifier/xref_contact_name_market_magnifier_merge.sql")

    ## address
    task_xref_contact_address_merge = int_u.run_query('merge_address', "dml/integration/incremental/contact_info/market_magnifier/xref_contact_address_market_magnifier_merge.sql")

    ## phone
    task_xref_contact_phone_merge = int_u.run_query('merge_phone', "dml/integration/incremental/contact_info/market_magnifier/xref_contact_phone_market_magnifier_merge.sql")



    ## keys
    task_dim_contact_source_key_merge = int_u.run_query('merge_keys', "dml/integration/incremental/contact_info/market_magnifier/dim_contact_source_key_market_magnifier_merge.sql")

    # contact
    task_contact_insert = int_u.run_query('insert_contact', "dml/integration/incremental/contact_info/market_magnifier/dim_contact_info_market_magnifier_insert.sql")
    task_contact_merge = int_u.run_query('merge_contact', "dml/integration/incremental/contact_info/shared/dim_contact_info_merge.sql")
    task_contact_match_audit = int_u.run_query('match_audit', "dml/integration/incremental/contact_info/market_magnifier/audit_contact_match_market_magnifier_insert.sql")

    # demogr_person

    # membership_billing_summary - Workarea loads
    task_demogr_person_work_source = int_u.run_query("build_demo_person_work_source", "dml/integration/historical/demographic/demogr_person_work_source.sql")
    task_demogr_person_work_transformed = int_u.run_query("build_demo_person_work_transformed",
                                                          "dml/integration/historical/demographic/demogr_person_work_transformed.sql")

    # demogr_person - Merge Load
    task_demogr_person_insert = int_u.run_query('merge_demo_person', "dml/integration/historical/demographic/demogr_person_insert.sql")

    # demogr_insurance_score

    # membership_billing_summary - Workarea loads
    task_demogr_insurance_score_work_source = int_u.run_query("build_demo_insurance_score_work_source",
                                                              "dml/integration/historical/demographic/demogr_insurance_score_work_source.sql")
    task_demogr_insurance_score_work_transformed = int_u.run_query("build_demo_insurance_score_work_transformed",
                                                                   "dml/integration/historical/demographic/demogr_insurance_score_work_transformed.sql")

    # demogr_insurance_score - Merge Load
    task_demogr_insurance_score_insert = int_u.run_query('merge_demo_insurance_score', "dml/integration/historical/demographic/demogr_insurance_score_insert.sql")

    AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

    ######################################################################
    # DEPENDENCIES

task_name_insert >> task_contact_work_source
task_phone_insert >> task_contact_work_source
task_address_insert >> task_contact_work_source
    
task_contact_work_source >> task_contact_work_stage
task_contact_work_target >> task_contact_work_stage

task_contact_work_stage >> task_contact_insert

task_contact_insert >> task_xref_contact_name_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_address_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_phone_merge >> task_contact_merge
task_contact_insert >> task_dim_contact_source_key_merge >> task_contact_merge

task_contact_merge >> task_contact_match_audit
# demogr_person

task_contact_match_audit >> task_demogr_person_work_source >> task_demogr_person_work_transformed >> task_demogr_person_insert

# demogr_insurance_score

task_demogr_person_insert >> task_demogr_insurance_score_work_source >> task_demogr_insurance_score_work_transformed >> task_demogr_insurance_score_insert




AllTaskSuccess.set_upstream([task_demogr_person_insert, task_demogr_insurance_score_insert])