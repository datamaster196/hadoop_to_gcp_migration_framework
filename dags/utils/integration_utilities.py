from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


ENV = Variable.get("environment").lower()

INGESTION_PROJECT = f'adw-lake-{ENV}'
INTEGRATION_PROJECT = f'adw-{ENV}'

def run_query(task_id, query, retries=0):

    task = BigQueryOperator(task_id=task_id, sql=query, use_legacy_sql=False, retries=retries)

    return task
