#######################################################################################################################
##  DAG_NAME :  integrated_ers
##  PURPOSE :   Executes incremental load for ers model
##  PREREQUISITE DAGs :
#######################################################################################################################

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

DAG_TITLE = "integrated_ers"
SCHEDULE = Variable.get("integrated_ers_schedule", default_var='') or None

SENSOR_TIMEDELTA_D3 = Variable.get("integrated_ers_d3_timedelta", default_var='') or None

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

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, max_active_runs=1,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
    ######################################################################
    # TASKS 

    sensor_ingest_d3_task = ExternalTaskSensor(task_id='sensor_ingest_d3',
                                                external_dag_id='ingest_d3',
                                                external_task_id='AllTaskSuccess',
                                                #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_D3]),
                                                execution_delta=timedelta(minutes=30),
                                                AirflowSensorTimeout=10 * 60,
                                                check_existance=True,
                                                dag=dag
                                                )

    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_dim_name', "dml/integration/incremental/dim_name/d3/dim_name_d3.sql")

    # Dimensions
    ## phone
    task_phone_insert = int_u.run_query('insert_dim_phone', "dml/integration/incremental/dim_phone/d3/dim_phone_d3.sql")

    # Dimensions
    ## address
    task_address_insert = int_u.run_query('insert_dim_address',
                                          "dml/integration/incremental/dim_address/d3/dim_address_d3.sql")

    # Contact Matching Logic
    task_contact_work_source = int_u.run_query("build_contact_work_source",
                                               "dml/integration/incremental/contact_info/d3/contact_d3_work_source.sql")
    task_contact_work_target = int_u.run_query("build_contact_work_target",
                                               "dml/integration/incremental/contact_info/d3/contact_d3_work_target.sql")
    task_contact_work_stage = int_u.run_query("build_contact_work_matched",
                                              "dml/integration/incremental/contact_info/d3/contact_d3_work_matching.sql")

    # Contact XREF's
    ## name
    task_xref_contact_name_merge = int_u.run_query('merge_name',
                                                   "dml/integration/incremental/contact_info/d3/xref_contact_name_d3_merge.sql")

    ## address
    task_xref_contact_address_merge = int_u.run_query('merge_address',
                                                      "dml/integration/incremental/contact_info/d3/xref_contact_address_d3_merge.sql")

    ## phone
    task_xref_contact_phone_merge = int_u.run_query('merge_phone',
                                                    "dml/integration/incremental/contact_info/d3/xref_contact_phone_d3_merge.sql")

    ## keys
    task_dim_contact_source_key_merge = int_u.run_query('merge_keys',
                                                        "dml/integration/incremental/contact_info/d3/dim_contact_source_key_d3_merge.sql")

    # contact
    task_contact_insert = int_u.run_query('insert_contact',
                                          "dml/integration/incremental/contact_info/d3/dim_contact_info_d3_insert.sql")
    task_contact_merge = int_u.run_query('merge_contact',
                                         "dml/integration/incremental/contact_info/shared/dim_contact_info_merge.sql")
    task_contact_match_audit = int_u.run_query('match_audit',
                                               "dml/integration/incremental/contact_info/d3/audit_contact_match_d3_insert.sql")

    # fact_roadside_service_call
    task_fact_roadside_service_call_work_source = int_u.run_query('fact_roadside_service_call_work_source',
                                                                  "dml/integration/incremental/fact_roadside_service_call/fact_roadside_service_call_work_source.sql")
    task_fact_roadside_service_call_transformed = int_u.run_query('fact_roadside_service_call_transformed',
                                                                  "dml/integration/incremental/fact_roadside_service_call/fact_roadside_service_call_work_transformed.sql")
    task_fact_roadside_service_call_stage = int_u.run_query('fact_roadside_service_call_stage',
                                                            "dml/integration/incremental/fact_roadside_service_call/fact_roadside_service_call_work_stage.sql")
    task_fact_roadside_service_call_merge = int_u.run_query('fact_roadside_service_call_merge',
                                                            "dml/integration/incremental/fact_roadside_service_call/fact_roadside_service_call.sql")

    ######################################################################
    # DEPENDENCIES

    task_name_insert.set_upstream([sensor_ingest_d3_task])
    task_phone_insert.set_upstream([sensor_ingest_d3_task])
    task_address_insert.set_upstream([sensor_ingest_d3_task])
    task_contact_work_target.set_upstream([sensor_ingest_d3_task])


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

    task_contact_match_audit >> task_fact_roadside_service_call_work_source >> task_fact_roadside_service_call_transformed >> task_fact_roadside_service_call_stage >> task_fact_roadside_service_call_merge

    AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

    AllTaskSuccess.set_upstream([task_fact_roadside_service_call_merge])