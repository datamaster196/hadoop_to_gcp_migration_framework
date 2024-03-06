#######################################################################################################################
##  DAG_NAME :  ingest_OptOut_Data_single_file
##  PURPOSE :   DAG to load data from source to ingest for AAA Optout
##  PREREQUISITE DAGs : ingest_eds
#######################################################################################################################

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from utils import ingestion_utilities as iu
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = 'ingest_OptOut_Data_single_file'
SCHEDULE = Variable.get("optout_ingest_schedule", default_var='') or None

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
AUTO_MMDDYYYYHHMM = Variable.get("optout_mmddyyyyhhmm_auto_yn", default_var='') or None
SRC_MMDDYYYYHHMM = Variable.get("optout_source_file_mmddyyyyhhmm", default_var='') or None

OO_SRCFILE_DTTM = ""
OO_SNAPSHOT_DTTM = ""

OO_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d %H:%M:00") }}'

OO_SRCFILE_DTTM = SRC_MMDDYYYYHHMM[11:15] + '-' + SRC_MMDDYYYYHHMM[7:9] + '-' + SRC_MMDDYYYYHHMM[
                                                                                9:11] + ' ' + SRC_MMDDYYYYHHMM[
                                                                                              15:17] + ':' + SRC_MMDDYYYYHHMM[
                                                                                                             17:19] + ':' + '00'

if AUTO_MMDDYYYYHHMM.upper() == 'N':
    if SRC_MMDDYYYYHHMM:
        OO_SNAPSHOT_DTTM = OO_SRCFILE_DTTM
    else:
        OO_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d 00:00:00") }}'
elif AUTO_MMDDYYYYHHMM.upper() == 'Y':
    OO_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d 00:00:00") }}'
else:
    OO_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d 00:00:00") }}'

CURRENT_DT = '{{ execution_date.strftime("%m%d%Y") }}'

optout_update = f"""
update `{iu.INGESTION_PROJECT}.aaacom.aaacomoptout` 
   set adw_lake_insert_datetime =  cast(\'%s\' as datetime),
       adw_lake_insert_batch_number = {{{{ dag_run.id }}}} 
 where adw_lake_insert_batch_number is null
""" % (OO_SNAPSHOT_DTTM)

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, max_active_runs=1)

if AUTO_MMDDYYYYHHMM.upper() == 'N':
    bashcmd = """bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="" --field_delimiter=\"TAB\" --skip_leading_rows=1 --allow_jagged_rows aaacom.aaacomoptout gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/Optout/%s \n \
                 if [ $? != 0 ] \n \
                   then echo \"BQ load failed\"; echo $VAR1; exit 1 \n \
                   else echo \"BQ load completed, File loaded: {{ SRC_MMDDYYYYHHMM }}\" ; VAR2=$(gsutil rm gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/Optout/%s) \n \
                 fi \n \
             """ % (SRC_MMDDYYYYHHMM, SRC_MMDDYYYYHHMM)
else:
    bashcmd = """VAR=$(gsutil ls gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/Optout/OptOut_{{ execution_date.strftime("%m%d%Y") }}'*'.txt | sort | head -1 ) 2> null && \
           if [ $VAR != "" ] \n \
           then \
               VAR1=$(bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker=\"\" --field_delimiter=\"TAB\" --skip_leading_rows=1 --allow_jagged_rows aaacom.aaacomoptout `echo $VAR` ) \n \
               if [ $? != 0 ] \n \
                 then echo \"BQ load failed\"; echo $VAR1; exit 1 \n \
                 else echo \"BQ load completed, File loaded: $VAR\" ;  VAR2=$(gsutil rm `echo $VAR` ) \n \
               fi \n \
           else echo \"Failing the load. Optput datafile for current date does not exist in source folder\"; exit 1 \n \
           fi"""

# Load GCS to BQ
GCStoBQ = BashOperator(task_id='GCStoBQ',
                       bash_command=bashcmd,
                       dag=dag)

# Update BQ Table BatchNo
BQUpdate = BigQueryOperator(task_id='BQUpdate',
                            sql=optout_update,
                            use_legacy_sql=False,
                            start_date=datetime(2019, 8, 22),
                            retries=0)

# Clean up file in Airflow
# RemoveFileGCS = BashOperator(task_id='RemoveFileGCS',
#                                bash_command='gsutil rm {{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/Optout/AAAComOptOuts.txt',
#                                dag=dag)

# Set Sequence
GCStoBQ >> BQUpdate
# GCStoBQ >> BQUpdate >> RemoveFileGCS

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([BQUpdate])