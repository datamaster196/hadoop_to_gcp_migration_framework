from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from utils import ingestion_utilities as iu

SOURCE_DB_NAMES = ['epic', 'd3', 'mzp', 'aca_store_history']
DAG_TITLE = 'setup_env_ingest'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'params': {
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


def gcs_make_bucket(dag, bucket_name):
    task_id = f'create_bucket_{bucket_name}'
    cmd = f"gsutil mb -p {iu.INGESTION_PROJECT} -l {iu.REGION} -b off gs://{bucket_name}"

    task = BashOperator(task_id=task_id, bash_command=cmd, dag=dag)

    return task


dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=None)

db_cleanup = BigQueryOperator(
    task_id='bq_db_env_cleanup',
    #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
    sql='sql/dbcleanup/bq_ingestion_dbcleanup.sql',
    use_legacy_sql=False,
    dag=dag
)

# dummy loop, one iteration
for admin in ['admin']:

    # get configurations of admin tables
    admin_table_config_dict = {c['table_name']: c for c in iu.read_webserver_table_file_list('admin')}
    ingestion_batch_config = admin_table_config_dict['ingestion_batch']
    ingestion_hwm_config = admin_table_config_dict['ingestion_highwatermarks']

    # db_cleanup = bq_db_cleanup(dag, f'adw-lake-{iu.ENV}'.lower())

    for source_db in SOURCE_DB_NAMES:

        # create the bq dataset and gs bucket
        make_dataset_ingest = bq_make_dataset(dag, source_db)
        make_bucket_ingest = gcs_make_bucket(dag, f'adw-lake-{source_db}-{iu.ENV}'.lower())

        for table_config in iu.read_webserver_table_file_list(source_db):
            # create the table, add record to admin table
            create_table = bq_make_table(dag, table_config)
            create_hwm_record = bq_add_hwm_entry(dag, table_config)

            make_dataset_ingest >> create_table
            bq_make_dataset(dag, 'admin') >> bq_make_table(dag, ingestion_batch_config) >> bq_make_table(dag, ingestion_hwm_config) >> create_hwm_record
