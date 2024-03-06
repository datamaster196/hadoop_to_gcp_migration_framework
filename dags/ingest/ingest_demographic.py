#######################################################################################################################
##  DAG_NAME :  ingest_demographic
##  PURPOSE :   DAG to load data from source to ingest for demographic model
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

DAG_TITLE = 'ingest_demographic'
SCHEDULE = Variable.get("demographic_ingest_schedule", default_var='') or None

AUTO_YYYYMM = Variable.get("market_magnifier_yyyymm_auto_yn", default_var='') or None
SRC_YYYYMM = Variable.get("market_magnifier_source_file_yyyymm", default_var='') or None

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


MM_SRCFILE_YYYYMM = ""
MM_SNAPSHOT_DTTM = ""


if AUTO_YYYYMM.upper() == 'N':
   if SRC_YYYYMM:
      MM_SRCFILE_YYYYMM = SRC_YYYYMM
      MM_SNAPSHOT_DTTM = SRC_YYYYMM[:4]+ '-' + SRC_YYYYMM[4:] + '-01 00:00:00'
   else:
      MM_SRCFILE_YYYYMM = '{{ execution_date.strftime("%Y%m") }}'
      MM_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}'
elif AUTO_YYYYMM.upper() == 'Y':
   MM_SRCFILE_YYYYMM = '{{ execution_date.strftime("%Y%m") }}'
   MM_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}'
else:
    MM_SRCFILE_YYYYMM = '{{ execution_date.strftime("%Y%m") }}'
    MM_SNAPSHOT_DTTM = '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}'

marketmagnifier_update = f"""
update `{iu.INGESTION_PROJECT}.demographic.marketing_magnifier` 
   set adw_lake_insert_datetime =  cast(\'%s\' as datetime),
       adw_lake_insert_batch_number = {{{{ dag_run.id }}}} where adw_lake_insert_batch_number is null
""" % (MM_SNAPSHOT_DTTM)


dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6, max_active_runs=1)

# Copy file from GCS to Airflow
GCSToAirFlow = BashOperator(task_id='GCSToAirFlow',
                               bash_command='gsutil cp gs://goanywhere_file_repo/{{ var.value.environment }}/MarketMagnifier/212_MarketMagnifier_wNixieCD_' + MM_SRCFILE_YYYYMM + '.zip gs://{{ var.value.airflow_gcs_bucket }}/212_MarketMagnifier_wNixieCD_' + MM_SRCFILE_YYYYMM + '.zip',
                               dag=dag)

# Unzip the file in Airflow
Unzip = BashOperator(task_id='Unzip',
                        bash_command='unzip -j -o -q /home/airflow/gcs/data/212_MarketMagnifier_wNixieCD_' + MM_SRCFILE_YYYYMM + '.zip 212_mm_wNixieCD.txt -d /home/airflow/gcs/data/',
                        dag=dag)

# Copy the file back to GCS
AirflowtoGCS = BashOperator(task_id='AirflowtoGCS',
                               bash_command='gsutil cp gs://{{ var.value.airflow_gcs_bucket }}/212_mm_wNixieCD.txt gs://goanywhere_file_repo/{{ var.value.environment }}/MarketMagnifier/212_mm_wNixieCD.txt',
                               dag=dag)

# Load GCS to BQ
GCStoBQ = BashOperator(task_id='GCStoBQ',
                          bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="TAB" --skip_leading_rows=1 --allow_jagged_rows demographic.marketing_magnifier gs://goanywhere_file_repo/{{ var.value.environment }}/MarketMagnifier/212_mm_wNixieCD.txt',
                          dag=dag)

# Update BQ Table BatchNo
BQUpdate = BigQueryOperator(task_id='BQUpdate',
                               sql=marketmagnifier_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

# Clean up file in Airflow
RemoveFileAirflowZip = BashOperator(task_id='RemoveFileAirflowZip',
                                bash_command='gsutil rm gs://{{ var.value.airflow_gcs_bucket }}/212_MarketMagnifier_wNixieCD_' + MM_SRCFILE_YYYYMM + '.zip',
                                dag=dag)
# Clean up file in Airflow
RemoveFileAirflow = BashOperator(task_id='RemoveFileAirflow',
                                bash_command='gsutil rm gs://{{ var.value.airflow_gcs_bucket }}/212_mm_wNixieCD.txt',
                                dag=dag)
# Clean up file in Airflow
RemoveFileGCS = BashOperator(task_id='RemoveFileGCS',
                                bash_command='gsutil rm gs://goanywhere_file_repo/{{ var.value.environment }}/MarketMagnifier/212_mm_wNixieCD.txt',
                                dag=dag)

#trigger_dag = TriggerDagRunOperator(
#    task_id="integrated_demogr",
#    trigger_dag_id="integrated_demogr",  # Ensure this equals the dag_id of the DAG to trigger
#    conf={"message": "integrated_demogr has been triggered"},
#    dag=dag)

# Set Sequence
#GCSToAirFlow >> Unzip >> AirflowtoGCS >> GCStoBQ >> BQUpdate >>RemoveFileAirflowZip >> RemoveFileAirflow >> RemoveFileGCS >> trigger_dag
GCSToAirFlow >> Unzip >> AirflowtoGCS >> GCStoBQ >> BQUpdate >>RemoveFileAirflowZip >> RemoveFileAirflow >> RemoveFileGCS
AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

#AllTaskSuccess.set_upstream([trigger_dag])
AllTaskSuccess.set_upstream([RemoveFileGCS])