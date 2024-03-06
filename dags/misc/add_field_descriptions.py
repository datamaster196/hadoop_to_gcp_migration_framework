#######################################################################################################################
##  DAG_NAME :  UTIL-DATA-STEWARD-FIELD-DESCRIPTIONS
##  PURPOSE :   DAG to add field description for data steward
##  PREREQUISITE DAGs :
#######################################################################################################################

from airflow import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import logging
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None


def load_data(**kwargs):
    client = bigquery.Client()
    logging.info("starting")
    logging.info(kwargs['INGESTION_PROJECT'])
    #Retrieves the list of tables derived from the prior step
    table_query = ("SELECT DISTINCT table_catalog, table_schema, table_name FROM `{}.admin.staged_descriptions`".format(kwargs['INGESTION_PROJECT']))
    #Start the BQ job
    table_job = client.query(table_query,location="US",)
    #For each table returned
    for row in table_job:
        table_id = '{}.{}.{}'.format(row.table_catalog, row.table_schema, row.table_name)
        print(table_id)
        table = client.get_table(table_id)
        new_schema = []
        # Retrieves the fields for the current table with the new descriptions to be added
        field_query = (
            "SELECT name, type, mode, description FROM `{}.admin.staged_descriptions` WHERE table_schema = '{}' and table_name = '{}'".format(kwargs['INGESTION_PROJECT'], row.table_schema, row.table_name))
        #Starts the subquery
        field_job = client.query(field_query, location="US", )
        #Adds each field to the new schema object. All fields from the originating table must be included. Fields which are in the
        #stewards google sheet but are not currently in the table will be ignored. data types and mode are also ignored   .. Took out field_row.type, field_row.mode,
        # can't do that Adnan. Added them back --Bill
        for field_row in field_job:
            new_schema.append(bigquery.SchemaField(field_row.name, field_row.type, field_row.mode, field_row.description))
        #Applys the new schema object to the table object
        table.schema = new_schema
        #Patches the table with the new schema
        table = client.update_table(table, ["schema"])

        print("Patched Table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id))


DAG_TITLE = "UTIL-DATA-STEWARD-FIELD-DESCRIPTIONS"

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

dag = DAG('UTIL-DATA-STEWARD-FIELD-DESCRIPTIONS',
          default_args=default_args,
          description='Field descriptions for tables in the ADW datasets',
          schedule_interval=None,
          catchup=False)

staged_descriptions = BigQueryOperator(
    task_id='staged_descriptions',
    bql='sql/dml/misc/field_descriptions.sql',
    destination_dataset_table='{{var.value.INGESTION_PROJECT}}.admin.staged_descriptions',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

patch_table = PythonOperator(python_callable=load_data, task_id='patch_table',
                             op_kwargs={'INGESTION_PROJECT': Variable.get('INGESTION_PROJECT')},dag=dag)

staged_descriptions >> patch_table

AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([patch_table])