#######################################################################################################################
##  DAG_NAME :  integrated_cdl_notification
##  PURPOSE :   Sends notification on completion of outbound DAGs outbound_adw_recipient_cdl, outbound_adw_cdl, outbound_insurance_cdl
##  PREREQUISITE DAGs : outbound_adw_recipient_cdl
##                      outbound_adw_cdl
##                      outbound_insurance_cdl
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

SUCCESS_EMAIL = Variable.get("email_cdl_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "integrated_cdl_notification"
SCHEDULE = Variable.get("integrated_cdl_notification_schedule", default_var='') or None

EASTERN = pytz.timezone('US/Eastern')
EASTERN_TIME = datetime.now(EASTERN)

SENSOR_TIMEDELTA_INS = Variable.get("integrated_cdl_notification_ins_timedelta", default_var='') or None
SENSOR_TIMEDELTA_ADW = Variable.get("integrated_cdl_notification_adw_timedelta", default_var='') or None
SENSOR_TIMEDELTA_RECPT = Variable.get("integrated_cdl_notification_recipient_timedelta", default_var='') or None



EMAIL_SUBJECT="INFO: CDL {{var.value.environment}} - CDL integration completed successfully"
EMAIL_BODY="""
'<h3>Hello CDL user, <br>
<br>
           The Campaign Data Layer (CDL) has completed processing data as of %s. The CDL is ready for use.<br>
<br>
                   If you find a data issue, please open a service now incident and assign to the Application Development and Data Services team.<br>
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
    sensor_outbound_recipient_task = ExternalTaskSensor(task_id='sensor_outbound_recipient',
                                     external_dag_id='outbound_adw_recipient_cdl',
                                     external_task_id='AllTaskSuccess',
                                     #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_RECPT]),
                                     execution_delta=timedelta(minutes=180),
                                     AirflowSensorTimeout=10 * 60,
                                     check_existance=True,
                                     dag=dag
                                     )

    sensor_outbound_adw_task = ExternalTaskSensor(task_id='sensor_outbound_adw',
                                     external_dag_id='outbound_adw_cdl',
                                     external_task_id='AllTaskSuccess',
                                     #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_ADW]),
                                     execution_delta=timedelta(minutes=180),
                                     AirflowSensorTimeout=10 * 60,
                                     check_existance=True,
                                     dag=dag
                                     )

    sensor_outbound_insurance_task = ExternalTaskSensor(task_id='sensor_outbound_insurance',
                                     external_dag_id='outbound_insurance_cdl',
                                     external_task_id='AllTaskSuccess',
                                     #execution_delta = timedelta(minutes=[SENSOR_TIMEDELTA_INS]),
                                     execution_delta=timedelta(minutes=180),
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
AllTaskSuccess.set_upstream([sensor_outbound_insurance_task, sensor_outbound_adw_task, sensor_outbound_recipient_task])