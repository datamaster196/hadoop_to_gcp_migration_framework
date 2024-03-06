#######################################################################################################################
##  DAG_NAME :  integrated_demogr
##  PURPOSE :   Executes incremental load for demographic model
##  PREREQUISITE DAGs :
#######################################################################################################################

#Initial load, adw_row_has, batch_number, history vs incremental, level 2??, what if Membership_ky is null?
from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task_sensor import ExternalTaskSensor

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "integrated_demogr"
SCHEDULE = Variable.get("integrated_demogr_schedule", default_var='') or None

SENSOR_TIMEDELTA_DEMOGR = Variable.get("integrated_demogr_demogr_timedelta", default_var='') or None

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

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, template_searchpath='/home/airflow/gcs/dags/sql/', max_active_runs=1)  as dag:
    ######################################################################
    # TASKS

    sensor_ingest_demographic_task = ExternalTaskSensor(task_id='sensor_ingest_demographic',
                                                external_dag_id='ingest_demographic',
                                                external_task_id='AllTaskSuccess',
                                                #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_DEMOGR]),
                                                execution_delta=timedelta(minutes=420),
                                                AirflowSensorTimeout=10 * 60,
                                                check_existance=True,
                                                dag=dag
                                                )

    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_dim_name', "dml/integration/incremental/dim_name/market_magnifier/dim_name_market_magnifier.sql")

    ## phone
    task_phone_insert = int_u.run_query('insert_dim_phone', "dml/integration/incremental/dim_phone/market_magnifier/dim_phone_market_magnifier.sql")

    ## address
    task_address_insert = int_u.run_query('insert_dim_address', "dml/integration/incremental/dim_address/market_magnifier/dim_address_market_magnifier.sql")

	
    # Contact Matching Logic 
    task_contact_work_source = int_u.run_query("build_contact_work_source","dml/integration/incremental/contact_info/market_magnifier/contact_market_magnifier_work_source.sql")
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

    # demogr_person - Workarea loads
    task_demogr_person_work_source = int_u.run_query("build_demo_person_work_source", "dml/integration/incremental/demographic/demogr_person_work_source.sql")
    task_demogr_person_work_transformed = int_u.run_query("build_demo_person_work_transformed", "dml/integration/incremental/demographic/demogr_person_work_transformed.sql")
    task_demogr_person_work_final_staging = int_u.run_query("build_demo_person_work_final_staging", "dml/integration/incremental/demographic/demogr_person_work_final_staging.sql")

    # member - Merge Load
    task_demogr_person_merge = int_u.run_query("merge_demo_person", "dml/integration/incremental/demographic/demogr_person_merge.sql")

    ######################################################################

    # demogr_insurance_score

    # demogr_insurance_score - Workarea loads
    task_demogr_insurance_score_work_source = int_u.run_query("build_demo_insurance_score_work_source",
                                                              "dml/integration/incremental/demographic/demogr_insurance_score_work_source.sql")
    task_demogr_insurance_score_work_transformed = int_u.run_query("build_demo_insurance_score_work_transformed",
                                                                   "dml/integration/incremental/demographic/demogr_insurance_score_work_transformed.sql")
    task_demogr_insurance_score_work_final_staging = int_u.run_query("build_demo_insurance_score_work_final_staging",
                                                                     "dml/integration/incremental/demographic/demogr_insurance_score_work_final_staging.sql")

    # member - Merge Load
    task_demogr_insurance_score_merge = int_u.run_query("merge_demo_insurance_score", "dml/integration/incremental/demographic/demogr_insurance_score_merge.sql")

    AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

    
    ######################################################################
    # DEPENDENCIES
task_name_insert.set_upstream([sensor_ingest_demographic_task])
task_phone_insert.set_upstream([sensor_ingest_demographic_task])
task_address_insert.set_upstream([sensor_ingest_demographic_task])
task_contact_work_target.set_upstream([sensor_ingest_demographic_task])

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
   

task_contact_match_audit >> task_demogr_person_work_source >> task_demogr_person_work_transformed >> task_demogr_person_work_final_staging >> task_demogr_person_merge


# demogr_insurance_score

task_demogr_person_merge >> task_demogr_insurance_score_work_source >> task_demogr_insurance_score_work_transformed >> task_demogr_insurance_score_work_final_staging >> task_demogr_insurance_score_merge



AllTaskSuccess.set_upstream([task_demogr_insurance_score_merge])
    