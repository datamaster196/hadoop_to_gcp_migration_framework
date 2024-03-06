#######################################################################################################################
##  DAG_NAME :  INGEST_DAG_RUN_TABLE_BQ
##  PURPOSE :   DAG to import DAG run ids from Airflow to ingestion_batch_dag_run
##  PREREQUISITE DAGs :
#######################################################################################################################

from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from utils import ingestion_utilities as iu
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule


SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = "INGEST_DAG_RUN_TABLE_BQ"
SCHEDULE = Variable.get("dag_run_bq_schedule", default_var='') or None

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

dag = DAG("INGEST_DAG_RUN_TABLE_BQ", default_args=default_args, schedule_interval=SCHEDULE, catchup=False)

class TableConfig:
    STANDARD_EXPORT_QUERY = None
    _STANDARD_EXPORT_QUERY = "SELECT * from {}"

    def __init__(self,
                 cloud_sql_instance,
                 export_bucket,
                 export_database,
                 export_table,
                 export_query,
                 gcp_project,
                 stage_dataset,
                 stage_table,
                 stage_final_query
                 ):

        self.params = {
            'export_table': export_table,
            'export_bucket': export_bucket,
            'export_database': export_database,
            'export_query': export_query or self._STANDARD_EXPORT_QUERY.format(export_table),
            'gcp_project': gcp_project,
            'stage_dataset': stage_dataset,
            'stage_table': stage_table or export_table,
            'stage_final_query': stage_final_query,
            'cloud_sql_instance': cloud_sql_instance,
        }


def get_tables():
    export_tables = 'dag_run'
    tables = []
    for dim in export_tables:
        tables.append(TableConfig(cloud_sql_instance='CLOUD_SQL_INSTANCE_NAME',
                                  export_table='',
                                  export_bucket='',
                                  export_database='adw-lake-{{var.value.environment}}',
                                  export_query=TableConfig.STANDARD_EXPORT_QUERY,
                                  gcp_project="",
                                  stage_dataset="",
                                  stage_table=None,
                                  stage_final_query=None
                                  ))
    return tables

def gen_export_table_task(table_config):
    export_task = MySqlToGoogleCloudStorageOperator(task_id='export_dag_run_to_gcs',
                                                    dag=dag,
                                                    sql='select id,dag_id,execution_date,state,end_date,start_date from dag_run',
                                                    bucket='adw-lake-{{var.value.environment}}-ingest-dag-run',
                                                    filename="dag_run_output_{{ ts_nodash }}",
                                                    google_cloud_storage_conn_id='google_cloud_storage_default',
                                                    mysql_conn_id='airflow_db')
    return export_task


def gen_import_table_task(table_config):
    import_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dag_run_to_bigquery',
        bucket='adw-lake-{{var.value.environment}}-ingest-dag-run',
        source_objects=['dag_run_output_{{ ts_nodash }}'],
        destination_project_dataset_table="adw-lake-{{var.value.environment}}.admin.ingestion_batch_dag_run",
        schema_fields=[
  {
    "mode": "NULLABLE",
    "name": "id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "dag_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "execution_date",
    "type": "TIMESTAMP"
  },
  {
    "mode": "NULLABLE",
    "name": "state",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "run_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "external_trigger",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "conf",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "end_date",
    "type": "TIMESTAMP"
  },
  {
    "mode": "NULLABLE",
    "name": "start_date",
    "type": "TIMESTAMP"
  }
],
        write_disposition='WRITE_TRUNCATE',
        source_format="NEWLINE_DELIMITED_JSON",
        dag=dag)

    return import_task


"""
The code that follows setups the dependencies between the tasks
"""

for table_config in get_tables():
    export_script = gen_export_table_task(table_config)
    import_script = gen_import_table_task(table_config)

    export_script >> import_script

    AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

    AllTaskSuccess.set_upstream([import_script])