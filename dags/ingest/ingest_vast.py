#######################################################################################################################
##  DAG_NAME :  ingest_vast
##  PURPOSE :   DAG to load data from source to ingest for VAST model
##  PREREQUISITE DAGs : ingest_eds
#######################################################################################################################

from datetime import datetime, timedelta, date

from airflow import DAG
from utils import ingestion_utilities as iu
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

SOURCE_DB_NAME = 'vast'
DAG_TITLE = 'ingest_vast'
SCHEDULE = Variable.get("vast_ingest_schedule", default_var='') or None


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
    'file_partition': iu.build_file_partition(),
    'params': {
        'dag_name': DAG_TITLE
    }
}

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6, max_active_runs=1)

# grab table configuration data and highwatermark control table data
config_cluster = iu.read_webserver_cluster_config(SOURCE_DB_NAME)
config_db = iu.read_webserver_datasource_config(SOURCE_DB_NAME)
config_tables = iu.read_webserver_table_file_list(SOURCE_DB_NAME)

config_tables = iu.inject_dynamic_configs(SOURCE_DB_NAME, config_tables)

create_cluster = iu.create_dataproc_cluster(config_cluster, dag)
delete_cluster = iu.delete_dataproc_cluster(config_cluster, dag)



for table_config in config_tables:
    create_cluster >> \
    iu.submit_sqoop_job(config_cluster, config_db, table_config, dag) >> \
    iu.convert_to_utf8(dag, table_config) >> \
    iu.gcs_to_bq(dag, table_config) >> \
    iu.write_to_hwm_table(dag, table_config) >> \
    delete_cluster



AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([delete_cluster])