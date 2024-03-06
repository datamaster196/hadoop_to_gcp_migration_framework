#######################################################################################################################
##  DAG_NAME :  setup_env_ingest
##  PURPOSE :   Executes DDL to set up ingest layer
##  PREREQUISITE DAGs : UTIL-DROP-INGESTION-TABLES
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from utils import ingestion_utilities as iu
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

SOURCE_DB_NAMES = ['epic', 'd3', 'mzp', 'demographic','pos','vast','mrm','mba','cwp','chargers','ersbilling','aaacom','sigma', 'sfmc', 'csaa', 'd3cur', 'misc']
DAG_TITLE = 'setup_env_ingest'


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
        'dag_name': DAG_TITLE,
        'ingestion_project': iu.INGESTION_PROJECT
    }
}


def bq_make_dataset(dag, source_db, project=f'{iu.INGESTION_PROJECT}'):
    task_id = f'create-dataset-{iu.ENV}-{source_db}'
    cmd = f"""bq ls "{project}:{source_db}" || bq mk "{project}:{source_db}" """

    return BashOperator(task_id=task_id, dag=dag, bash_command=cmd)


def bq_make_table(dag, table_config):
    dataset_name = table_config['dataset_name']
    table_name = table_config['table_name']

    task_id = f'create-table-{iu.ENV}-{dataset_name}-{table_name}'
    ddl = table_config['ddl'].format(INGESTION_PROJECT=iu.INGESTION_PROJECT)

    cmd = f"bq query --use_legacy_sql=false '{ddl}'"

    return BashOperator(task_id=task_id, dag=dag, bash_command=cmd)


def bq_add_hwm_entry(dag, table_config):
    dataset = table_config['dataset_name']
    table = table_config['table_name']

    sql = f"""
    INSERT INTO `adw-lake-{iu.ENV}.admin.ingestion_highwatermarks` (dataset_name, table_name, highwatermark)

    with row as (
    SELECT "{dataset}", "{table}", ""
    )

    select * from row
    WHERE NOT EXISTS (
    SELECT * FROM `adw-lake-{iu.ENV}.admin.ingestion_highwatermarks`
    where dataset_name = "{dataset}"
    and table_name = "{table}"
    )"""

    cmd = f"bq query --use_legacy_sql=false '{sql}'"

    return BashOperator(task_id=f'add-initial-highwatermark-record-{table}-{iu.ENV}',
                        dag=dag, bash_command=cmd, retries=20)


#def gcs_make_bucket(dag, bucket_name):
#    task_id = f'create_bucket_{bucket_name}'
#    cmd = f"gsutil mb -p {iu.INGESTION_PROJECT} -l {iu.REGION} -b off gs://{bucket_name}"
#
#    task = BashOperator(task_id=task_id, bash_command=cmd, dag=dag)
#
#    return task

def gcs_make_bucket(dag, bucket_name):
    task_id = f'create_bucket_{bucket_name}'
    cmd = f" VAR=$(( gsutil ls gs://{bucket_name})| wc -l) 2> null &&  \n \
    if [ $VAR -eq 0 ]  \n \
       then VAR1=$( gsutil mb -p {iu.INGESTION_PROJECT} -l {iu.REGION} -b off gs://{bucket_name}) ; echo $VAR1 \n \
       else echo \"The bucket (gs://{bucket_name}) already exists\"  \n \
    fi"

    task = BashOperator(task_id=task_id, bash_command=cmd, dag=dag)

    return task

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=None, max_active_runs=1)

make_bucket_demographics = gcs_make_bucket(dag, f'adw-lake-demographics-{iu.ENV}'.lower())
make_bucket_adw_lake_ingest_dag_run = gcs_make_bucket(dag, f'adw-lake-{iu.ENV}-ingest-dag-run'.lower())

# dummy loop, one iteration
for admin in ['admin']:

    # get configurations of admin tables
    admin_table_config_dict = {c['table_name']: c for c in iu.read_webserver_table_file_list('admin')}
    ingestion_batch_config = admin_table_config_dict['ingestion_batch']
    ingestion_hwm_config = admin_table_config_dict['ingestion_highwatermarks']
    data_steward_config = admin_table_config_dict['data_steward']
    adw_record_count_validation_config = admin_table_config_dict['adw_record_count_validation']
    ingestion_batch_dag_run_config = admin_table_config_dict['ingestion_batch_dag_run']
    aca_store_list_config = admin_table_config_dict['aca_store_list']

    # Create work dataset in the ingest area
    make_dataset_ingest_work = bq_make_dataset(dag, 'work')
    make_dataset_ingest_work
    for source_db in SOURCE_DB_NAMES:

        # create the bq dataset and gs bucket
        make_dataset_ingest = bq_make_dataset(dag, source_db)
        make_bucket_ingest = gcs_make_bucket(dag, f'adw-lake-{source_db}-{iu.ENV}'.lower())

        for table_config in iu.read_webserver_table_file_list(source_db):
            # create the table, add record to admin table
            create_table = bq_make_table(dag, table_config)
            create_hwm_record = bq_add_hwm_entry(dag, table_config)

            make_dataset_ingest >> create_table
            bq_make_dataset(dag, 'admin') >> bq_make_table(dag, aca_store_list_config) >> bq_make_table(dag, ingestion_batch_config) >> bq_make_table(dag,
                                                                                                         ingestion_hwm_config) >> bq_make_table(
                dag, data_steward_config) >> bq_make_table(dag, adw_record_count_validation_config) >> bq_make_table(
                dag, ingestion_batch_dag_run_config) >> create_hwm_record

AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([create_hwm_record])