#######################################################################################################################
##  DAG_NAME :  ingest_mzp
##  PURPOSE :   DAG to load data from source to ingest for MZP model
##  PREREQUISITE DAGs : ingest_eds
#######################################################################################################################

from datetime import datetime, timedelta, date

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

SOURCE_DB_NAME = 'mzp'
DAG_TITLE = 'ingest_mzp'
SCHEDULE = Variable.get("mzp_ingest_schedule", default_var='') or None


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
    'execution_timeout': timedelta(minutes=120),
    'file_partition': iu.build_file_partition(),
    'params': {
        'dag_name': DAG_TITLE
    }
}


#dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6, template_searchpath='/home/airflow/gcs/dags/sql/')

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6,  max_active_runs=1,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
# grab table configuration data and highwatermark control table data
    config_cluster = iu.read_webserver_cluster_config(SOURCE_DB_NAME)
    config_db = iu.read_webserver_datasource_config(SOURCE_DB_NAME)
    config_tables = iu.read_webserver_table_file_list(SOURCE_DB_NAME)

    config_tables = iu.inject_dynamic_configs(SOURCE_DB_NAME, config_tables)

    create_cluster = iu.create_dataproc_cluster(config_cluster, dag)
    delete_cluster = iu.delete_dataproc_cluster(config_cluster, dag)

    task_src_count = int_u.run_query('mzp_src_count',"dml/ingest/mzp/record_count/src_count.sql")
    task_tgt_count = int_u.run_query('mzp_tgt_count',"dml/ingest/mzp/record_count/tgt_count.sql")

    task_mbrs_balance_create = int_u.run_query('mbrs_balance_create',"dml/ingest/mzp/membership_balance/create_membership_balance_work.sql")
    task_mbrs_balance_insert = int_u.run_query('mbrs_balance_insert',"dml/ingest/mzp/membership_balance/membership_balance.sql")


    task_pause = BashOperator(task_id='task_pause', bash_command='echo 1',dag=dag)

#    trigger_dag = TriggerDagRunOperator(
#        task_id="integrated_contact_mzp",
#        trigger_dag_id="integrated_contact_mzp",  # Ensure this equals the dag_id of the DAG to trigger
#        conf={"message": "integrated_contact_mzp has been triggered"},
#        dag=dag)

#    for table_config in config_tables:
#        task_mbrs_balance_create >> \
#        create_cluster >> \
#        iu.submit_sqoop_job(config_cluster, config_db, table_config, dag) >> \
#        iu.convert_to_utf8(dag, table_config) >> \
#        iu.gcs_to_bq(dag, table_config) >> \
#        iu.write_to_hwm_table(dag, table_config) >> \
#        delete_cluster
#

    for table_config in config_tables:

       if table_config['table_name'] != "adw_record_count_validation":
          task_mbrs_balance_create >> \
          create_cluster >> \
          iu.submit_sqoop_job(config_cluster, config_db, table_config, dag) >> \
          iu.convert_to_utf8(dag, table_config) >> \
          iu.gcs_to_bq(dag, table_config) >> \
          iu.write_to_hwm_table(dag, table_config) >> \
          task_pause >> \
          delete_cluster
       else:
          task_mbrs_balance_create >> \
          create_cluster >> \
          task_pause >> \
          iu.submit_sqoop_job(config_cluster, config_db, table_config, dag) >> \
          iu.convert_to_utf8(dag, table_config) >> \
          iu.gcs_to_bq(dag, table_config) >> \
          iu.write_to_hwm_table(dag, table_config) >> \
          delete_cluster

#   delete_cluster >> task_src_count >> task_tgt_count >> trigger_dag
    delete_cluster >> task_mbrs_balance_insert >> task_src_count >> task_tgt_count

AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

#AllTaskSuccess.set_upstream([trigger_dag])
AllTaskSuccess.set_upstream([task_tgt_count])
