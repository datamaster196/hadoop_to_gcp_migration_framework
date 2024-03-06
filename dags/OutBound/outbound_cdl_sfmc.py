#######################################################################################################################
##  DAG_NAME :  outbound_cdl_sfmc
##  PURPOSE :   Executes Outsbound DAG for CDL sfmc
##  PREREQUISITE DAGs :
#######################################################################################################################

import pendulum
from datetime import datetime, timedelta, date
from airflow import DAG
from utils import ingestion_utilities as iu
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

local_tz = pendulum.timezone("US/Eastern")
SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None
DAG_TITLE = 'outbound_cdl_sfmc'
SCHEDULE = Variable.get("outbound_cdl_sfmc_schedule", default_var='') or None
ENV = Variable.get("environment").lower()
CDL_SERVER = Variable.get("Cdl_Server")
CDL_DB = Variable.get("Cdl_db")
INGESTION_PROJECT = f'adw-lake-{ENV}'
INTEGRATION_PROJECT = f'adw-{ENV}'
CLUSTER_CONFIG_FOLDER = 'outbound_sfmc'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 1, 0, tzinfo=local_tz),
    'email': [FAIL_EMAIL],
    'catchup': False,
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    'file_partition': iu.build_file_partition(),
    'params': {
        'dag_name': DAG_TITLE
    }
}

# Initial validation check
query_init_check = f"""
    if exists (select * from `{INGESTION_PROJECT}.admin.ingestion_highwatermarks` where dataset_name = 'outbound_cdl_sfmc' and safe_cast(highwatermark as datetime) is null) then
      select error('highwatermark must be a valid datetime for "outbound_cdl_sfmc" - Please fix then restart');
    end if;
"""

# make a full copy of distinct open to cdl dataset
query_copy_sfmc_open = f"""
    CREATE OR REPLACE TABLE `{INTEGRATION_PROJECT}.cdl.sfmc_open` 
    AS  SELECT  accountid, oybaccountid, jobid, listid, batchid, subscriberid, subscriberkey, eventdate, domain, isunique, triggeredsendcustomerkey, 
                triggerersenddefinitionobjectid, adw_lake_insert_datetime, adw_lake_insert_batch_number 
        FROM (SELECT *, row_number() over(partition by jobid, listid, batchid, subscriberid, eventdate order by adw_lake_insert_datetime desc) as sequence 
              FROM `{INGESTION_PROJECT}.sfmc.open`
        ) as a
        WHERE sequence = 1;
"""

# make a full copy of distinct click to cdl dataset
query_copy_sfmc_click = f"""
    CREATE OR REPLACE TABLE `{INTEGRATION_PROJECT}.cdl.sfmc_click` 
    AS  SELECT  accountid, oybaccountid, jobid, listid, batchid, subscriberid, subscriberkey, eventdate, domain, url, linkname, linkcontent, isunique, 
                triggeredsendcustomerkey, triggerersenddefinitionobjectid, adw_lake_insert_datetime, adw_lake_insert_batch_number   
        FROM (SELECT *, row_number() over(partition by jobid, listid, batchid, subscriberid, eventdate, url, linkname order by adw_lake_insert_datetime desc) as sequence 
              FROM `{INGESTION_PROJECT}.sfmc.click`
        ) as a
        WHERE sequence = 1;
"""

# make a full copy of distinct sendlog to cdl dataset
query_copy_sfmc_sendlog = f"""
    CREATE OR REPLACE TABLE `{INTEGRATION_PROJECT}.cdl.sfmc_sendlog` 
    AS  SELECT  jobid, listid, batchid, subscriberid, triggeredsendid, errorcode, commid, messagesubject, emailaddress, senddate, subscriberkey, listname, 
                adw_lake_insert_datetime, adw_lake_insert_batch_number   
        FROM (SELECT *, row_number() over(partition by jobid, listid, batchid, subscriberid order by adw_lake_insert_datetime desc) as sequence 
              FROM `{INGESTION_PROJECT}.sfmc.sendlog`
        ) as a
        WHERE sequence = 1;
"""

# make a full copy of distinct subscribers to cdl dataset
query_copy_sfmc_subscribers = f"""
    CREATE OR REPLACE TABLE `{INTEGRATION_PROJECT}.cdl.sfmc_subscribers` 
    AS  SELECT  subscriberid, dateundeliverable, datejoined, dateunsubscribed, domain, emailaddress, bouncecount, subscriberkey, subscribertype, status, locale, 
                modifieddate, adw_lake_insert_datetime, adw_lake_insert_batch_number   
        FROM (SELECT *, row_number() over(partition by subscriberid order by adw_lake_insert_datetime desc) as sequence 
              FROM `{INGESTION_PROJECT}.sfmc.subscribers`
        ) as a
        WHERE sequence = 1;
"""

# in additional to incremental load, also restrict to activity within the last 2 months for transactional tables
# list of tuples with table name, condition, select (transformation possible), column names
table_list = [
    ("sfmc_sendlog",
     " and safe_cast(substr(senddate,1,10) as date) >= date_sub(current_date(), interval 2 month) ",
     "jobid, listid, batchid, subscriberid, triggeredsendid, errorcode, commid, trim(replace(messagesubject,'|','')) as messagesubject, emailaddress, senddate, subscriberkey, listname, adw_lake_insert_datetime, adw_lake_insert_batch_number",
     "jobid, listid, batchid, subscriberid, triggeredsendid, errorcode, commid, messagesubject, emailaddress, senddate, subscriberkey, listname, adw_lake_insert_datetime, adw_lake_insert_batch_number"
    ),
    ("sfmc_open",
     " and safe_cast(substr(eventdate,1,10) as date) >= date_sub(current_date(), interval 2 month) ",
     "jobid, listid, batchid, subscriberid, eventdate, domain, accountid, oybaccountid, subscriberkey, isunique, triggeredsendcustomerkey, triggerersenddefinitionobjectid, adw_lake_insert_datetime, adw_lake_insert_batch_number",
     "jobid, listid, batchid, subscriberid, eventdate, domain, accountid, oybaccountid, subscriberkey, isunique, triggeredsendcustomerkey, triggerersenddefinitionobjectid, adw_lake_insert_datetime, adw_lake_insert_batch_number"
    ),
    ("sfmc_click",
     " and safe_cast(substr(eventdate,1,10) as date) >= date_sub(current_date(), interval 2 month) ",
     "jobid, listid, batchid, subscriberid, eventdate, accountid, url, linkname, oybaccountid, subscriberkey, domain, linkcontent, isunique, triggeredsendcustomerkey, triggerersenddefinitionobjectid, adw_lake_insert_datetime, adw_lake_insert_batch_number",
     "jobid, listid, batchid, subscriberid, eventdate, accountid, url, linkname, oybaccountid, subscriberkey, domain, linkcontent, isunique, triggeredsendcustomerkey, triggerersenddefinitionobjectid, adw_lake_insert_datetime, adw_lake_insert_batch_number"
    ),
    ("sfmc_subscribers",
     " ",
     "subscriberid, if(safe_cast(dateundeliverable as datetime) is null,'null',dateundeliverable) as dateundeliverable, datejoined, if(safe_cast(dateunsubscribed as datetime) is null,'null',dateunsubscribed) as dateunsubscribed, domain, emailaddress, bouncecount, subscriberkey, subscribertype, status, locale, modifieddate, adw_lake_insert_datetime, adw_lake_insert_batch_number",
     "subscriberid, dateundeliverable, datejoined, dateunsubscribed, domain, emailaddress, bouncecount, subscriberkey, subscribertype, status, locale, modifieddate, adw_lake_insert_datetime, adw_lake_insert_batch_number"
    )
]

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6, max_active_runs=1)

# grab table configuration data
config_cluster = iu.read_webserver_cluster_config(CLUSTER_CONFIG_FOLDER)
create_cluster = iu.create_dataproc_cluster(config_cluster, dag)
delete_cluster = iu.delete_dataproc_cluster(config_cluster, dag)

init_check = BigQueryOperator(task_id="init_check", sql=query_init_check, dag=dag, use_legacy_sql=False, retries=0)
copy_sfmc_click = BigQueryOperator(task_id="copy_sfmc_click", sql=query_copy_sfmc_click, dag=dag, use_legacy_sql=False, retries=0)
copy_sfmc_open = BigQueryOperator(task_id="copy_sfmc_open", sql=query_copy_sfmc_open, dag=dag, use_legacy_sql=False, retries=0)
copy_sfmc_sendlog = BigQueryOperator(task_id="copy_sfmc_sendlog", sql=query_copy_sfmc_sendlog, dag=dag, use_legacy_sql=False, retries=0)
copy_sfmc_subscribers = BigQueryOperator(task_id="copy_sfmc_subscribers", sql=query_copy_sfmc_subscribers, dag=dag, use_legacy_sql=False, retries=0)

init_check >> [copy_sfmc_click, copy_sfmc_open, copy_sfmc_sendlog, copy_sfmc_subscribers] >> create_cluster

for itm in table_list:
    table = itm[0]
    condition = itm[1]
    select = itm[2]
    columns = itm[3]

    stg_table = f'stg_{table}'

    query_create_work_table = f"""
        CREATE or REPLACE table `{INTEGRATION_PROJECT }.adw_work.outbound_cdl_{table}_work` AS 
        SELECT {select} FROM `{INTEGRATION_PROJECT}.cdl.{table}` 
        WHERE adw_lake_insert_datetime > (select max(cast(highwatermark as datetime)) FROM `{INGESTION_PROJECT}.admin.ingestion_highwatermarks` WHERE dataset_name = 'outbound_cdl_sfmc' and table_name = '{table}') 
        {condition};
    """

    query_hwm_update = f"""
       UPDATE `{INGESTION_PROJECT}.admin.ingestion_highwatermarks` 
       SET highwatermark = (SELECT SAFE_CAST(MAX(adw_lake_insert_datetime) AS STRING) FROM `{INTEGRATION_PROJECT}.cdl.{table}`) 
       WHERE dataset_name = 'outbound_cdl_sfmc' and table_name = '{table}';
     """

    query_truncate_stg = f'TRUNCATE TABLE {stg_table}'
    query_load_sql = f'EXEC dbo.usp_load_{table};'

    create_work_table = BigQueryOperator(task_id=f"create_work_{table}", sql=query_create_work_table, dag=dag, use_legacy_sql=False, retries=0)

    bq_2_gcs = BashOperator(task_id=f"bq_2_gcs_{table}",
                     bash_command='bq extract --project_id={{params.integration_project}} --field_delimiter "|" --print_header=false adw_work.outbound_cdl_{{params.table}}_work gs://{{params.integration_project}}-outgoing/{{params.table}}/{{execution_date.strftime("%Y%m%d")}}/{{params.table}}_*.psv',
                     dag=dag,
                     params= {'integration_project': INTEGRATION_PROJECT, 'table': table})

    truncate_stg = BashOperator(task_id=f"truncate_{stg_table}",
                     bash_command='gcloud dataproc jobs submit hadoop --project={{params.ingestion_project}} --cluster="{{params.cluster_name}}" --region="us-east1" --class=org.apache.sqoop.Sqoop --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar" -- eval -Dmapreduce.job.user.classpath.first=true --connect="jdbc:jtds:sqlserver://{{params.cdl_server}}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{params.cdl_db}};" --username="SVCGCPUSER" --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{params.environment}}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver" --query "{{params.sql}}" ',
                     dag=dag,
                     params = {'ingestion_project': INGESTION_PROJECT,
                               'environment': ENV,
                               'cluster_name': config_cluster['cluster_name'],
                               'cdl_server': CDL_SERVER,
                               'cdl_db': CDL_DB,
                               'sql': query_truncate_stg})

    gcs_2_stg = BashOperator(task_id=f"gcs_2_{stg_table}",
                      bash_command='gcloud dataproc jobs submit hadoop --project={{params.ingestion_project}} --cluster="{{params.cluster_name}}" --region="us-east1" --class=org.apache.sqoop.Sqoop --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar" -- export -Dmapreduce.job.user.classpath.first=true --connect="jdbc:jtds:sqlserver://{{params.cdl_server}}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{params.cdl_db}};" --username="SVCGCPUSER" --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{params.environment}}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver" --table "{{params.stg_table}}" --export-dir "gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}" -m 4 -input-fields-terminated-by "|" --input-optionally-enclosed-by "\\"" --input-null-string "null" --input-null-non-string "-1" --columns "{{params.columns}}" ',
                      dag=dag,
                      params= {'ingestion_project': INGESTION_PROJECT,
                               'integration_project': INTEGRATION_PROJECT,
                               'environment': ENV,
                               'cluster_name': config_cluster['cluster_name'],
                               'cdl_server': CDL_SERVER,
                               'cdl_db': CDL_DB,
                               'table': table,
                               'stg_table': stg_table,
                               'columns': columns})

    load_from_stg = BashOperator(task_id=f"load_{table}",
                     bash_command='gcloud dataproc jobs submit hadoop --project={{params.ingestion_project}} --cluster="{{params.cluster_name}}" --region="us-east1" --class=org.apache.sqoop.Sqoop --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar" -- eval -Dmapreduce.job.user.classpath.first=true --connect="jdbc:jtds:sqlserver://{{params.cdl_server}}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{params.cdl_db}};" --username="SVCGCPUSER" --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{params.environment}}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver" --query "{{params.sql}}" ',
                     dag=dag,
                     params = {'ingestion_project': INGESTION_PROJECT,
                               'environment': ENV,
                               'cluster_name': config_cluster['cluster_name'],
                               'cdl_server': CDL_SERVER,
                               'cdl_db': CDL_DB,
                               'sql': query_load_sql})

    hwm_update = BigQueryOperator(task_id=f"hwm_update_{table}", sql=query_hwm_update, dag=dag, use_legacy_sql=False, retries=0)

    create_cluster >> create_work_table >> bq_2_gcs >> truncate_stg >> gcs_2_stg >> load_from_stg >>  hwm_update >> delete_cluster

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([delete_cluster])
