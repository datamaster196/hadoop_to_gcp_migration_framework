#######################################################################################################################
##  DAG_NAME :  ingest_sfmc
##  PURPOSE :   DAG to load data from source to ingest for SFMC model
##  PREREQUISITE DAGs : ingest_eds
#######################################################################################################################

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from utils import ingestion_utilities as iu
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = 'ingest_sfmc'
SCHEDULE = Variable.get("sfmc_ingest_schedule", default_var='') or None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 19),
    'email': [FAIL_EMAIL],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
    'params': {
        'dag_name': DAG_TITLE
    }
}

current_dt = date.today().strftime("%Y%m%d")

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6, max_active_runs=1)

sfmc_open_update = f"""
update `{iu.INGESTION_PROJECT}.sfmc.open` set adw_lake_insert_datetime =  CURRENT_DATETIME(),
adw_lake_insert_batch_number = {{{{ dag_run.id }}}}
where adw_lake_insert_batch_number is null
"""

# Format Open file from UTF-16 to UTF-8
FormatOpen = BashOperator(task_id='FormatOpen',
                               bash_command='gsutil cp gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Open_{{ params.current_dt }}.txt - | tr -d "\\000" | gsutil cp - gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Open_{{ params.current_dt }}.txt',
                               dag=dag,
                               params= {'current_dt': current_dt}
                          )
# Load GCS to BQ Open
GCStoBQOpen = BashOperator(task_id='GCStoBQOpen',
                          bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="," --skip_leading_rows=1 --allow_jagged_rows -E UTF-8 sfmc.open gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Open_{{ params.current_dt }}.txt',
                          dag=dag,
                          params= {'current_dt': current_dt}
                          )

# Update BQ Open Table BatchNo
BQOpenUpdate = BigQueryOperator(task_id='BQOpenUpdate',
                               sql=sfmc_open_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

sfmc_click_update = f"""
update `{iu.INGESTION_PROJECT}.sfmc.click` set adw_lake_insert_datetime =  CURRENT_DATETIME(),
adw_lake_insert_batch_number = {{{{ dag_run.id }}}}
where adw_lake_insert_batch_number is null
"""

# Format Click file from UTF-16 to UTF-8
FormatClick = BashOperator(task_id='FormatClick',
                               bash_command='gsutil cp gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Click_{{ params.current_dt }}.txt - | tr -d "\\000" | gsutil cp - gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Click_{{ params.current_dt }}.txt',
                               dag=dag,
                               params={'current_dt': current_dt}
                           )
# Load GCS to BQ Click
GCStoBQClick = BashOperator(task_id='GCStoBQClick',
                          bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="," --skip_leading_rows=1 --allow_jagged_rows -E UTF-8 sfmc.click gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Click_{{ params.current_dt }}.txt',
                          dag=dag,
                          params={'current_dt': current_dt}
                          )

# Update BQ Click Table BatchNo
BQClickUpdate = BigQueryOperator(task_id='BQClickUpdate',
                               sql=sfmc_click_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

sfmc_subscriber_update = f"""
update `{iu.INGESTION_PROJECT}.sfmc.subscribers` set adw_lake_insert_datetime =  CURRENT_DATETIME(),
adw_lake_insert_batch_number = {{{{ dag_run.id }}}}
where adw_lake_insert_batch_number is null
"""

# Format Click file from UTF-16 to UTF-8
FormatSubscriber = BashOperator(task_id='FormatSubscriber',
                               bash_command='gsutil cp gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Subscribers_{{ params.current_dt }}.txt - | tr -d "\\000" | gsutil cp - gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Subscribers_{{ params.current_dt }}.txt',
                               dag=dag,
                               params={'current_dt': current_dt}
                               )

# Load GCS to BQ Subscriber
GCStoBQSubscriber = BashOperator(task_id='GCStoBQSubscriber',
                                 bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="," --skip_leading_rows=1 --allow_jagged_rows -E UTF-8 sfmc.subscribers gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Subscribers_{{ params.current_dt }}.txt',
                                 dag=dag,
                                 params={'current_dt': current_dt}
                                 )

# Update BQ Subscriber Table BatchNo
BQSubscriberUpdate = BigQueryOperator(task_id='BQSubscriberUpdate',
                               sql=sfmc_subscriber_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

sfmc_unsubscribe_update = f"""
update `{iu.INGESTION_PROJECT}.sfmc.unsubscribe` set adw_lake_insert_datetime =  CURRENT_DATETIME(),
adw_lake_insert_batch_number = {{{{ dag_run.id }}}}
where adw_lake_insert_batch_number is null
"""
# Format Click file from UTF-16 to UTF-8
FormatUnsubscribe = BashOperator(task_id='FormatUnsubscribe',
                                bash_command='gsutil cp gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Unsubscribe_{{ params.current_dt }}.txt - | tr -d "\\000" | gsutil cp - gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Unsubscribe_{{ params.current_dt }}.txt',
                                dag=dag,
                                params={'current_dt': current_dt})

# Load GCS to BQ Subscriber
GCStoBQUnsubscribe = BashOperator(task_id='GCStoBQUnsubscribe',
                          bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="," --skip_leading_rows=1 --allow_jagged_rows -E UTF-8 sfmc.unsubscribe gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Unsubscribe_{{ params.current_dt }}.txt',
                          dag=dag,
                          params= {'current_dt': current_dt})

# Update BQ Unsubscribe Table BatchNo
BQUnsubscribeUpdate = BigQueryOperator(task_id='BQUnsubscribeUpdate',
                               sql=sfmc_unsubscribe_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

sfmc_bounce_update = f"""
update `{iu.INGESTION_PROJECT}.sfmc.bounce` set adw_lake_insert_datetime =  CURRENT_DATETIME(),
adw_lake_insert_batch_number = {{{{ dag_run.id }}}}
where adw_lake_insert_batch_number is null
"""
# Format Click file from UTF-16 to UTF-8
FormatBounce = BashOperator(task_id='FormatBounce',
                               bash_command='gsutil cp gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Bounce_{{ params.current_dt }}.txt - | tr -d "\\000" | gsutil cp - gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Bounce_{{ params.current_dt }}.txt',
                               dag=dag,
                               params= {'current_dt': current_dt})

# Load GCS to BQ Subscriber
GCStoBQBounce = BashOperator(task_id='GCStoBQBounce',
                          bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="," --skip_leading_rows=1 --allow_jagged_rows -E UTF-8 sfmc.bounce gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_Bounce_{{ params.current_dt }}.txt',
                          dag=dag,
                          params= {'current_dt': current_dt})

# Update BQ Unsubscribe Table BatchNo
BQBounceUpdate = BigQueryOperator(task_id='BQBounceUpdate',
                               sql=sfmc_bounce_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

sfmc_listsubscriber_update = f"""
update `{iu.INGESTION_PROJECT}.sfmc.listsubscribers` set adw_lake_insert_datetime =  CURRENT_DATETIME(),
adw_lake_insert_batch_number = {{{{ dag_run.id }}}}
where adw_lake_insert_batch_number is null
"""

# Format Click file from UTF-16 to UTF-8
FormatListSubscriber = BashOperator(task_id='FormatListSubscriber',
                               bash_command='gsutil cp gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_ListSubscribers_{{ params.current_dt }}.txt - | tr -d "\\000" | gsutil cp - gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_ListSubscribers_{{ params.current_dt }}.txt',
                               dag=dag,
                               params= {'current_dt': current_dt})

# Load GCS to BQ Subscriber
GCStoBQListSubscriber = BashOperator(task_id='GCStoBQListSubscriber',
                          bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="," --skip_leading_rows=1 --allow_jagged_rows -E UTF-8 sfmc.listsubscribers gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_ListSubscribers_{{ params.current_dt }}.txt',
                          dag=dag,
                          params= {'current_dt': current_dt})

# Update BQ Subscriber Table BatchNo
BQListSubscriberUpdate = BigQueryOperator(task_id='BQListSubscriberUpdate',
                               sql=sfmc_listsubscriber_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

sfmc_sendlog_update = f"""
update `{iu.INGESTION_PROJECT}.sfmc.sendlog` set adw_lake_insert_datetime =  CURRENT_DATETIME(),
adw_lake_insert_batch_number = {{{{ dag_run.id }}}}
where adw_lake_insert_batch_number is null
"""

# Format Click file from UTF-16 to UTF-8
FormatSendLog = BashOperator(task_id='FormatSendLog',
                               bash_command='gsutil cp gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_SendLog_{{ params.current_dt }}.txt - | tr -d "\\000" | gsutil cp - gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_SendLog_{{ params.current_dt }}.txt',
                               dag=dag,
                               params= {'current_dt': current_dt})

# Load GCS to BQ Subscriber
GCStoBQSendLog = BashOperator(task_id='GCStoBQSendLog',
                          bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --null_marker="null" --field_delimiter="," --skip_leading_rows=1 --allow_jagged_rows -E UTF-8 sfmc.sendlog gs://{{ var.value.go_anywhere_bucket }}/{{ var.value.environment }}/Marketing/SFMC/DV_SendLog_{{ params.current_dt }}.txt',
                          dag=dag,
                          params= {'current_dt': current_dt})

# Update BQ Subscriber Table BatchNo
BQSendLogUpdate = BigQueryOperator(task_id='BQSendLogUpdate',
                               sql=sfmc_sendlog_update,
                               use_legacy_sql=False,
                               start_date= datetime(2019, 8, 22),
                               retries=0)

# Clean up file in Airflow
MoveFilesArchive = BashOperator(task_id='MoveFilesArchive',
                                bash_command='gsutil mv gs://goanywhere_file_repo/{{ var.value.environment }}/Marketing/SFMC/*.txt gs://goanywhere_file_repo/{{ var.value.environment }}/Marketing/SFMC/archive',
                                dag=dag)

# Set Sequence
FormatOpen >> GCStoBQOpen >> BQOpenUpdate
FormatClick >> GCStoBQClick >> BQClickUpdate
FormatSubscriber >> GCStoBQSubscriber >> BQSubscriberUpdate
FormatUnsubscribe >> GCStoBQUnsubscribe >> BQUnsubscribeUpdate
FormatBounce >> GCStoBQBounce >> BQBounceUpdate
FormatListSubscriber >> GCStoBQListSubscriber >> BQListSubscriberUpdate
FormatSendLog >> GCStoBQSendLog >> BQSendLogUpdate

MoveFilesArchive.set_upstream([BQOpenUpdate])
MoveFilesArchive.set_upstream([BQClickUpdate])
MoveFilesArchive.set_upstream([BQSubscriberUpdate])
MoveFilesArchive.set_upstream([BQUnsubscribeUpdate])
MoveFilesArchive.set_upstream([BQListSubscriberUpdate])
MoveFilesArchive.set_upstream([BQSendLogUpdate])
MoveFilesArchive.set_upstream([BQBounceUpdate])

AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

#AllTaskSuccess.set_upstream([BQOpenUpdate, BQClickUpdate, BQSubscriberUpdate, BQUnsubscribeUpdate, BQBounceUpdate, BQListSubscriberUpdate])
AllTaskSuccess.set_upstream([MoveFilesArchive])