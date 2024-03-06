#######################################################################################################################
##  DAG_NAME :  integrated_final_notification
##  PURPOSE :   Sends final email notification on completion of daily incremental DAGs
##  PREREQUISITE DAGs : integrated_membership_mzp
##                      integrated_insurance
##                      integrated_ers
##                      integrated_demogr
##                      integrated_retail
##                      integrated_car_care
##                      integrated_marketing
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
import pytz

SUCCESS_EMAIL = Variable.get("email_final_intg_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "integrated_final_notification"
SCHEDULE = Variable.get("integrated_final_notification_schedule", default_var='') or None
EASTERN = pytz.timezone('US/Eastern')
EASTERN_TIME = datetime.now(EASTERN)


EMAIL_SUBJECT="INFO: ADW {{var.value.environment}} - ADW integration completed successfully"
EMAIL_BODY="""
'<h3>Hello ADW user, <br>
<br>
           The Alliance Data Warehouse (ADW) has completed processing data as of %s. The ADW is ready for use. This includes the adw and adw_pii base data sets and does not include downstream data marts or BI refreshes.<br>
<br>
                   The domains that processed are: <br>
                      D3, EPIC, MZP, SFMC, AAA Optout, POS & VAST <br>
<br>
                   If you find a data issue, please open a service now incident and assign to the Application Development and Data Services team.<br>
<br>
                   For information on the ADW, please see the <a href = "http://aaawebapps03.aaacorp.com/edwmetadata/" target = "_blank">ACA DATA Warehouse Metadata site</a><br>
<br>
Thank You <br>
Enterprise Data Services Team</h3>'
""" % (EASTERN_TIME.strftime("%Y-%m-%d %H:%M:%r %Z"))

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
    # Sensor Tasks
#    sensor_integrated_ers_task = ExternalTaskSensor(task_id='sensor_integrated_ers',
#                                     external_dag_id='integrated_ers',
#                                     external_task_id='AllTaskSuccess',
#                                     execution_delta = timedelta(minutes=150),
#                                     AirflowSensorTimeout=10 * 60,
#                                     check_existance=True,
#                                     dag=dag
#                                     )

#    sensor_demographic_task = ExternalTaskSensor(task_id='sensor_demographic',
#                                     external_dag_id='integrated_demogr',
#                                     external_task_id='AllTaskSuccess',
#                                     execution_delta = timedelta(minutes=9),
#                                     AirflowSensorTimeout=10 * 60,
#                                     check_existance=True,
#                                     dag=dag
#                                     )

#    sensor_insurance_task = ExternalTaskSensor(task_id='sensor_insurance',
#                                     external_dag_id='integrated_insurance',
#                                     external_task_id='AllTaskSuccess',
#                                     execution_delta = timedelta(minutes=9),
#                                     AirflowSensorTimeout=10 * 60,
#                                     check_existance=True,
#                                     dag=dag
#                                     )

#    sensor_membership_task = ExternalTaskSensor(task_id='sensor_membership',
#                                     external_dag_id='integrated_membership_mzp',
#                                     external_task_id='AllTaskSuccess',
#                                     execution_delta = timedelta(minutes=9),
#                                     AirflowSensorTimeout=10 * 60,
#                                     check_existance=True,
#                                     dag=dag
#                                     )

#    sensor_retail_task = ExternalTaskSensor(task_id='sensor_retail',
#                                     external_dag_id='integrated_retail',
#                                     external_task_id='AllTaskSuccess',
#                                     execution_delta = timedelta(minutes=9),
#                                     AirflowSensorTimeout=10 * 60,
#                                     check_existance=True,
#                                     dag=dag
#                                     )

#    sensor_car_care_task = ExternalTaskSensor(task_id='sensor_car_care',
#                                     external_dag_id='integrated_car_care',
#                                     external_task_id='AllTaskSuccess',
#                                     execution_delta = timedelta(minutes=9),
#                                     AirflowSensorTimeout=10 * 60,
#                                     check_existance=True,
#                                     dag=dag
#                                     )

    sensor_marketing_task = ExternalTaskSensor(task_id='sensor_marketing',
                                     external_dag_id='integrated_marketing',
                                     external_task_id='AllTaskSuccess',
                                     execution_delta = timedelta(minutes=45),
                                     AirflowSensorTimeout=10 * 60,
                                     check_existance=True,
                                     dag=dag
                                     )

    AllTaskSuccess = EmailOperator(
       dag=dag,
       trigger_rule=TriggerRule.ALL_SUCCESS,
       task_id="AllTaskSuccess",
       to=SUCCESS_EMAIL,
       subject=EMAIL_SUBJECT,
       html_content=EMAIL_BODY)

    ######################################################################
    # DEPENDENCIES

#AllTaskSuccess.set_upstream([])

#AllTaskSuccess.set_upstream([sensor_integrated_ers_task, sensor_demographic_task, sensor_insurance_task, sensor_membership_task, sensor_retail_task, sensor_car_care_task, sensor_marketing_task])
#AllTaskSuccess.set_upstream([sensor_integrated_ers_task, sensor_insurance_task, sensor_membership_task, sensor_retail_task, sensor_car_care_task, sensor_marketing_task])
AllTaskSuccess.set_upstream([sensor_marketing_task])