#######################################################################################################################
##  DAG_NAME :  integrated_marketing
##  PURPOSE :   Executes incremental load for marketing model
##  PREREQUISITE DAGs :
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from airflow import models

from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.sensors.external_task_sensor import ExternalTaskSensor


SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None
SCHEDULE = Variable.get("integrated_marketing_schedule", default_var='') or None

SENSOR_TIMEDELTA_OPTOUT = Variable.get("integrated_marketing_aaaoptout_timedelta", default_var='') or None
SENSOR_TIMEDELTA_SFMC = Variable.get("integrated_marketing_sfmc_timedelta", default_var='') or None

SENSOR_TIMEDELTA_INTG_MZP = Variable.get("integrated_marketing_intg_mzp_timedelta", default_var='') or None
SENSOR_TIMEDELTA_INTG_INS = Variable.get("integrated_marketing_intg_ins_timedelta", default_var='') or None
SENSOR_TIMEDELTA_INTG_ERS = Variable.get("integrated_marketing_intg_ers_timedelta", default_var='') or None
SENSOR_TIMEDELTA_INTG_RTL = Variable.get("integrated_marketing_intg_rtl_timedelta", default_var='') or None
SENSOR_TIMEDELTA_INTG_CAR_CARE = Variable.get("integrated_marketing_intg_car_care_timedelta", default_var='') or None


DAG_TITLE = "integrated_marketing"

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


with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False,max_active_runs=1 ,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
    sfmc_sensor_task = ExternalTaskSensor(task_id='sfmc_sensor',
                                     external_dag_id='ingest_sfmc',
                                     external_task_id='AllTaskSuccess',
                                     #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_SFMC]),
                                     execution_delta=timedelta(minutes=45),
                                     AirflowSensorTimeout=10 * 60,
                                     check_existance=True,
                                     dag=dag
                                     )

    aaacom_sensor_task = ExternalTaskSensor(task_id='aaacom_sensor',
                                          external_dag_id='ingest_OptOut_Data_single_file',
                                          external_task_id='AllTaskSuccess',
                                          #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_OPTOUT]),
                                          execution_delta=timedelta(minutes=180),
                                          AirflowSensorTimeout=10 * 60,
                                          check_existance=True,
                                          dag=dag
                                          )

    integrated_membership_sensor_task = ExternalTaskSensor(task_id='integrated_membership_sensor',
                                          external_dag_id='integrated_membership_mzp',
                                          external_task_id='AllTaskSuccess',
                                          #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_INTG_MZP]),
                                          execution_delta=timedelta(minutes=120),
                                          AirflowSensorTimeout=10 * 60,
                                          check_existance=True,
                                          dag=dag
                                          )

    integrated_insurance_sensor_task = ExternalTaskSensor(task_id='integrated_insurance_sensor',
                                          external_dag_id='integrated_insurance',
                                          external_task_id='AllTaskSuccess',
                                          #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_INTG_INS]),
                                          execution_delta=timedelta(minutes=600),
                                          AirflowSensorTimeout=10 * 60,
                                          check_existance=True,
                                          dag=dag
                                          )

    integrated_ers_sensor_task = ExternalTaskSensor(task_id='integrated_ers_sensor',
                                          external_dag_id='integrated_ers',
                                          external_task_id='AllTaskSuccess',
                                          #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_INTG_ERS]),
                                          execution_delta=timedelta(minutes=60),
                                          AirflowSensorTimeout=10 * 60,
                                          check_existance=True,
                                          dag=dag
                                          )

    integrated_retail_sensor_task = ExternalTaskSensor(task_id='integrated_retail_sensor',
                                          external_dag_id='integrated_retail',
                                          external_task_id='AllTaskSuccess',
                                          #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_INTG_RTL]),
                                          execution_delta=timedelta(minutes=30),
                                          AirflowSensorTimeout=10 * 60,
                                          check_existance=True,
                                          dag=dag
                                          )

    integrated_car_care_sensor_task = ExternalTaskSensor(task_id='integrated_car_care_sensor',
                                          external_dag_id='integrated_car_care',
                                          external_task_id='AllTaskSuccess',
                                          #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_INTG_CAR_CARE]),
                                          execution_delta=timedelta(minutes=90),
                                          AirflowSensorTimeout=10 * 60,
                                          check_existance=True,
                                          dag=dag
                                          )

    ######################################################################
    # TASKS

    # sfmc_fact_contact_event_optout
    task_sfmc_source = int_u.run_query('sfmc_fact_contact_event_optout_source',"dml/integration/incremental/fact_contact_event_optout/sfmc_fact_contact_event_optout/sfmc_fact_contact_event_optout_work_source.sql")
    task_sfmc_transformed = int_u.run_query('sfmc_fact_contact_event_optout_transformed',"dml/integration/incremental/fact_contact_event_optout/sfmc_fact_contact_event_optout/sfmc_fact_contact_event_optout_work_transform.sql")
    task_sfmc_work_final_staging = int_u.run_query("sfmc_fact_contact_event_optout_work_final_staging","dml/integration/incremental/fact_contact_event_optout/sfmc_fact_contact_event_optout/sfmc_fact_contact_event_optout_work_stage.sql")
    task_sfmc_insert = int_u.run_query('sfmc_fact_contact_event_optout_insert', "dml/integration/incremental/fact_contact_event_optout/sfmc_fact_contact_event_optout/sfmc_fact_contact_event_optout.sql")

    # aaacom_fact_contact_event_optout

    task_aaacom_source = int_u.run_query('aaacom_fact_contact_event_optout_source',
                                         "dml/integration/incremental/fact_contact_event_optout/aaacom_fact_contact_event_optout/aaacom_fact_contact_event_optout_work_source.sql")
    task_aaacom_transformed = int_u.run_query('aaacom_fact_contact_event_optout_transformed',
                                              "dml/integration/incremental/fact_contact_event_optout/aaacom_fact_contact_event_optout/aaacom_fact_contact_event_optout_work_transform.sql")
    task_aaacom_work_final_staging = int_u.run_query("aaacom_fact_contact_event_optout_work_final_staging",
                                                     "dml/integration/incremental/fact_contact_event_optout/aaacom_fact_contact_event_optout/aaacom_fact_contact_event_optout_work_stage.sql")
    task_aaacom_insert = int_u.run_query('aaacom_fact_contact_event_optout_insert',"dml/integration/incremental/fact_contact_event_optout/aaacom_fact_contact_event_optout/aaacom_fact_contact_event_optout.sql")

    # member_fact_contact_event_optout

    task_member_source = int_u.run_query('member_fact_contact_event_optout_source',"dml/integration/incremental/fact_contact_event_optout/member_fact_contact_event_optout/member_fact_contact_event_optout_work_source.sql")
    task_member_transformed = int_u.run_query('member_fact_contact_event_optout_transformed',"dml/integration/incremental/fact_contact_event_optout/member_fact_contact_event_optout/member_fact_contact_event_optout_work_transform.sql")
    task_member_work_final_staging = int_u.run_query("member_fact_contact_event_optout_work_final_staging","dml/integration/incremental/fact_contact_event_optout/member_fact_contact_event_optout/member_fact_contact_event_optout_work_stage.sql")
    task_member_insert = int_u.run_query('member_fact_contact_event_optout_insert', "dml/integration/incremental/fact_contact_event_optout/member_fact_contact_event_optout/member_fact_contact_event_optout.sql")

    # audit_member_fact_contact_event_optout

    task_audit_mem_source = int_u.run_query('audit_mem_fact_contact_event_optout_source',
                                            "dml/integration/incremental/fact_contact_event_optout/audit_mem_fact_contact_event_optout/audit_mem_fact_contact_event_optout_work_source.sql")
    task_audit_mem_transformed = int_u.run_query('audit_mem_fact_contact_event_optout_transformed',
                                                 "dml/integration/incremental/fact_contact_event_optout/audit_mem_fact_contact_event_optout/audit_mem_fact_contact_event_optout_work_transform.sql")
    task_audit_mem_work_final_staging = int_u.run_query("audit_mem_fact_contact_event_optout_work_final_staging",
                                                        "dml/integration/incremental/fact_contact_event_optout/audit_mem_fact_contact_event_optout/audit_mem_fact_contact_event_optout_work_stage.sql")
    task_audit_mem_insert = int_u.run_query('audit_mem_fact_contact_event_optout_insert',
                                            "dml/integration/incremental/fact_contact_event_optout/audit_mem_fact_contact_event_optout/audit_mem_fact_contact_event_optout.sql")

    # dim_contact_optout
    #task_contact_optout_pre_source = int_u.run_query('contact_optout_pre_source',
    #                                      "dml/integration/incremental/dim_contact_optout/contact_optout_work_pre_source.sql")
    #task_contact_optout_source = int_u.run_query('contact_optout_source',
    #                                      "dml/integration/incremental/dim_contact_optout/contact_optout_work_source.sql")
    #task_contact_optout_pre_transformed = int_u.run_query('contact_optout_pre_transformed',
    #                                           "dml/integration/incremental/dim_contact_optout/contact_optout_work_pre_transformed.sql")
    #
    #task_contact_optout_transformed = int_u.run_query('contact_optout_transformed',
    #                                           "dml/integration/incremental/dim_contact_optout/contact_optout_work_transformed.sql")
    #task_contact_optout_stage = int_u.run_query('contact_optout_stage',
    #                                     "dml/integration/incremental/dim_contact_optout/contact_optout_work_stage.sql")
    #task_contact_optout_merge = int_u.run_query('contact_optout_merge', "dml/integration/incremental/dim_contact_optout/dim_contact_optout.sql")

    # dim_contact_optout
    task_contact_optout_pre_source = int_u.run_query('contact_optout_pre_source',
                                                     "dml/integration/historical/dim_contact_optout/contact_optout_work_pre_source.sql")

    task_contact_optout_source = int_u.run_query('contact_optout_source',
                                                 "dml/integration/historical/dim_contact_optout/contact_optout_work_source.sql")
    task_contact_optout_transformed = int_u.run_query('contact_optout_transformed',
                                                      "dml/integration/historical/dim_contact_optout/contact_optout_work_transformed.sql")
    task_contact_optout_typ_2_hist = int_u.run_query('contact_optout_work_typ_2_hist',
                                                     "dml/integration/historical/dim_contact_optout/contact_optout_work_type_2_hist.sql")
    task_contact_optout_insert = int_u.run_query('contact_optout_insert',
                                                 "dml/integration/historical/dim_contact_optout/dim_contact_optout.sql")

AllTaskSuccess = EmailOperator(
       dag=dag,
       trigger_rule=TriggerRule.ALL_SUCCESS,
       task_id="AllTaskSuccess",
       to=SUCCESS_EMAIL,
       subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
       html_content='<h3>All Task completed successfully" </h3>')
    ######################################################################
    # DEPENDENCIES

# sfmc_fact_contact_event_optout
#sfmc_sensor_task.set_downstream(task_sfmc_source)

sfmc_sensor_task.set_downstream(task_sfmc_source)
aaacom_sensor_task.set_downstream(task_sfmc_source)
integrated_membership_sensor_task.set_downstream(task_sfmc_source)
integrated_insurance_sensor_task.set_downstream(task_sfmc_source)
integrated_ers_sensor_task.set_downstream(task_sfmc_source)
integrated_retail_sensor_task.set_downstream(task_sfmc_source)
integrated_car_care_sensor_task.set_downstream(task_sfmc_source)

task_sfmc_source >> task_sfmc_transformed >> task_sfmc_work_final_staging >> task_sfmc_insert

# aaacom_fact_contact_event_optout
#aaacom_sensor_task.set_downstream(task_aaacom_source)

sfmc_sensor_task.set_downstream(task_aaacom_source)
aaacom_sensor_task.set_downstream(task_aaacom_source)
integrated_membership_sensor_task.set_downstream(task_aaacom_source)
integrated_insurance_sensor_task.set_downstream(task_aaacom_source)
integrated_ers_sensor_task.set_downstream(task_aaacom_source)
integrated_retail_sensor_task.set_downstream(task_aaacom_source)
integrated_car_care_sensor_task.set_downstream(task_aaacom_source)

task_aaacom_source >> task_aaacom_transformed >> task_aaacom_work_final_staging
[task_sfmc_insert,task_aaacom_work_final_staging] >> task_aaacom_insert

# member_fact_contact_event_optout
#member_sensor_task.set_downstream([task_member_source,task_audit_mem_source])

sfmc_sensor_task.set_downstream([task_member_source,task_audit_mem_source])
aaacom_sensor_task.set_downstream([task_member_source,task_audit_mem_source])
integrated_membership_sensor_task.set_downstream([task_member_source,task_audit_mem_source])
integrated_insurance_sensor_task.set_downstream([task_member_source,task_audit_mem_source])
integrated_ers_sensor_task.set_downstream([task_member_source,task_audit_mem_source])
integrated_retail_sensor_task.set_downstream([task_member_source,task_audit_mem_source])
integrated_car_care_sensor_task.set_downstream([task_member_source,task_audit_mem_source])

task_member_source >> task_member_transformed >> task_member_work_final_staging
[task_aaacom_insert,task_member_work_final_staging] >> task_member_insert

# audit_member_fact_contact_event_optout
task_audit_mem_source >> task_audit_mem_transformed >> task_audit_mem_work_final_staging
[task_member_insert,task_audit_mem_work_final_staging] >> task_audit_mem_insert

# dim_contact_optout
#[task_audit_mem_insert] >> task_contact_optout_pre_source >> task_contact_optout_source >> task_contact_optout_pre_transformed >>  task_contact_optout_transformed >> task_contact_optout_stage >> task_contact_optout_merge
#AllTaskSuccess.set_upstream([task_contact_optout_merge])

# dim_contact_optout
[task_audit_mem_insert] >> task_contact_optout_pre_source >> task_contact_optout_source  >> task_contact_optout_transformed >> task_contact_optout_typ_2_hist >> task_contact_optout_insert
AllTaskSuccess.set_upstream([task_contact_optout_insert])

