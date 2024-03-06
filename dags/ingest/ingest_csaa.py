#######################################################################################################################
##  DAG_NAME :  ingest_csaa
##  PURPOSE :   DAG to load data from source to ingest for csaa
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

DAG_TITLE = 'ingest_csaa'
SCHEDULE = Variable.get("csaa_ingest_schedule", default_var='') or None

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
AUTO_YYYYMM = Variable.get("csaa_yyyymm_auto_yn", default_var='') or None
SRC_YYYYMM = Variable.get("csaa_source_file_yyyymm", default_var='') or None
OO_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d %H:%M:00") }}'


csaa_update = f"""
update `{iu.INGESTION_PROJECT}.csaa.csaa_panel` 
   set adw_lake_insert_datetime =  cast(\'%s\' as datetime),
       adw_lake_insert_batch_number = {{{{ dag_run.id }}}} 
 where adw_lake_insert_batch_number is null
""" % (OO_SNAPSHOT_DTTM)

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, max_active_runs=1)

if AUTO_YYYYMM.upper() == 'N':
    bashcmd = """bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="" --field_delimiter=\"|\" --skip_leading_rows=1 --allow_jagged_rows csaa.csaa_panel gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/csaa/%s \n \
                 if [ $? != 0 ] \n \
                   then echo \"BQ load failed\"; echo $VAR1; exit 1 \n \
                   else echo \"BQ load completed, File loaded: {{ SRC_YYYYMM }}\" ; VAR2=$(gsutil mv gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/csaa/%s gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/csaa/archive/) \n \
                 fi \n \
             """ % (SRC_YYYYMM, SRC_YYYYMM)
else:
    bashcmd = """VAR=$(gsutil ls gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/csaa/member_sstp1_cs2_ACA_{{ execution_date.strftime("%Y%m") }}.txt | sort | head -1 ) 2> null && \
           if [ $VAR != "" ] \n \
           then \
               VAR1=$(bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker=\"\" --field_delimiter=\"|\" --skip_leading_rows=1 --allow_jagged_rows csaa.csaa_panel `echo $VAR` ) \n \
               if [ $? != 0 ] \n \
                 then echo \"BQ load failed\"; echo $VAR1; exit 1 \n \
                 else echo \"BQ load completed, File loaded: $VAR\" ;  VAR2=$(gsutil mv `echo $VAR` gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/csaa/archive/) \n \
               fi \n \
           else echo \"Failing the load. csaa panel file for current date does not exist in source folder\"; exit 1 \n \
           fi  \n \
           """

# Load GCS to BQ
GCStoBQ = BashOperator(task_id='GCStoBQ',
                       bash_command=bashcmd,
                       dag=dag)

# Update BQ Table BatchNo
BQUpdate = BigQueryOperator(task_id='BQUpdate',
                            sql=csaa_update,
                            use_legacy_sql=False,
                            start_date=datetime(2019, 8, 22),
                            retries=0)

# Set Sequence
GCStoBQ >> BQUpdate

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([BQUpdate])