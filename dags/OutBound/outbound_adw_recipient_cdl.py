#######################################################################################################################
##  DAG_NAME :  outbound_adw_recipient_cdl
##  PURPOSE :   Executes Outsbound DAG for CDL adw recipient
##  PREREQUISITE DAGs :
#######################################################################################################################

from airflow.utils.dates import timezone
import pendulum
from datetime import datetime, timedelta, date
from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None
ENV = Variable.get("environment").lower()

INGESTION_PROJECT = f'adw-lake-{ENV}'
INTEGRATION_PROJECT = f'adw-{ENV}'

local_tz = pendulum.timezone("US/Eastern")

config_tables = {
   'cdl_recipient': '"iaddrerrorcount, iaddrquality, iblacklist, iblacklistemail, iblacklistfax, iblacklistmobile, iblacklistphone, iblacklistpostalmail, iboolean1, iboolean2, iboolean3, iemailformat, ifolderid, igender, irecipientid, istatus, mdata, saccount, saddress1, saddress2, saddress3, saddress4, scity, scompany, scountrycode, semail, sfax, sfirstname, slanguage, slastname, smiddlename, smobilephone, sorigin, sphone, ssalutation, sstatecode, stext1, stext2, stext3, stext4, stext5, szipcode, tsaddrlastcheck, tsbirth, tscreated, tslastmodified, contact_adw_key"'}

SOURCE_DB_NAME = 'adw_pii'
DAG_TITLE = 'outbound_adw_recipient_cdl'
SCHEDULE = Variable.get("outbound_recipient_cdl_schedule", default_var='') or None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22, 0, tzinfo=local_tz),
    'email': [FAIL_EMAIL],
    'catchup': False,
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120),
    'file_partition': iu.build_file_partition(),
    'params': {
        'dag_name': DAG_TITLE
    }
}

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6, max_active_runs=1,
          template_searchpath='/home/airflow/gcs/dags/sql/')

# grab table configuration data and highwatermark control table data
config_cluster = iu.read_webserver_cluster_config(SOURCE_DB_NAME)

create_cluster = iu.create_dataproc_cluster(config_cluster, dag)
delete_cluster = iu.delete_dataproc_cluster(config_cluster, dag)
#recipientload = int_u.run_query('recipientload', "dml/outbound/cdl/recipient/contactinfo2recipient.sql")



for table, fields in config_tables.items():
    cleanup_work_source = BigQueryOperator(task_id=f"cleanup_{table}_work_source",
                                           sql="DELETE FROM `{{ params.integration_project }}.cdl.{{ params.table }}` WHERE 1=1  ",
                                           dag=dag,
                                           use_legacy_sql=False,
                                           retries=0,
                                           params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                                   'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                                   'table': table,
                                                   'fields': fields})

    prepare_work_source = BigQueryOperator(task_id=f"prepare_{table}_work_source",
                                           sql="INSERT INTO `{{ params.integration_project }}.cdl.{{ params.table }}` (iGender,sAddress3,sAddress4,sCity,sCountryCode,sEmail,sFirstName,sLastName,sMiddleName,sPhone,sSalutation,sStateCode,sZipCode,stext1,tsAddrLastCheck,tsBirth, tscreated, tslastmodified, contact_adw_key) select  ifnull(CASE WHEN a.contact_gender_cd  = 'M' then '1' when a.contact_gender_cd  = 'F' then '2' else  '0' end ,'null'),ifnull(a.contact_address_1_nm,'null'),ifnull(a.contact_address_2_nm,'null'),ifnull(a.contact_city_nm,'null'),ifnull(a.contact_country_nm,'null'),ifnull(a.contact_email_nm,'null'),ifnull(a.contact_first_nm,'null'),ifnull(a.contact_last_nm,'null'),ifnull(a.contact_middle_nm,'null'),ifnull(a.contact_phone_nbr,'null'),ifnull(a.contact_title_nm,'null'), ifnull(a.contact_state_cd,'null'),ifnull(a.contact_postal_cd,'null'),ifnull(d.Mebrid16 ,'null'),c.integrate_update_datetime,ifnull(safe_cast(a.contact_birth_dt as string),'null') as contact_birth_dt, a.integrate_insert_datetime, a.integrate_update_datetime, ifnull(a.contact_adw_key,'null') from `{{ params.integration_project }}.adw_pii.dim_contact_info` a left join  ( select contact_adw_key ,  address_Adw_key, ROW_NUMBER() over (PARTITION BY contact_adw_key order by effective_start_datetime desc) DUPE_CHECK  FROM  `{{ params.integration_project }}.adw.xref_contact_address`  where actv_ind = 'Y' ) b on a.contact_adw_key = b.contact_adw_key and  b.dupe_check = 1 left join  `{{ params.integration_project }}.adw_pii.dim_address` c on b.address_adw_key = c.address_adw_key  left join (SELECT contact_adw_key, concat(ifnull(source_1_key,'') ,ifnull(source_2_key,'') ,ifnull(source_3_key,'') ,ifnull(source_4_key,'') ,ifnull(source_5_key,'')) as Mebrid16 FROM `{{ params.integration_project }}.adw.dim_contact_source_key` WHERE contact_source_system_nm = 'mzp' AND key_typ_nm LIKE 'member_id%' and actv_ind = 'Y' ) d  on a.contact_adw_key = d.contact_adw_key",
                                           dag=dag,
                                           use_legacy_sql=False,
                                           retries=0,
                                           params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                                   'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                                   'table': table,
                                                   'fields': fields})

    bq_2_gcs = BashOperator(task_id=f"bq_2_gcs_{table}",
                             bash_command='bq extract --project_id={{ params.integration_project }}  --field_delimiter "|" --print_header=false cdl.{{ params.table }} gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}/{{ params.table }}_*.psv',
                             dag=dag,
                             params= {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                      'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                      'table': table,
                                      'fields': fields})
    audit_start_date = BashOperator(task_id=f"audit_start_date_{table}",
                             bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-adw-pii"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "update dbo.cdl_load_status  set [load_start_dtm] = CURRENT_TIMESTAMP , [load_complete_dtm] = null where table_name = '"'"'{{ params.table }}'"'"' "    ',
                             dag=dag,
                             params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                     'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                     'environment': Variable.get("environment"),
                                     'Cdl_Server': Variable.get("Cdl_Server"),
                                     'Cdl_db': Variable.get("Cdl_db"),
                                     'table': table,
                                     'fields': fields})

    setup_sql = BashOperator(task_id=f"setup_sql_{table}",
                             bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-adw-pii"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "TRUNCATE TABLE dbo.{{ params.table }}"    ',
                             dag=dag,
                             params = {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                       'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                       'environment': Variable.get("environment"),
                                       'Cdl_Server':Variable.get("Cdl_Server"),
                                       'Cdl_db': Variable.get("Cdl_db"),
                                       'table': table,
                                       'fields': fields})
    gcs_2_sql = BashOperator(task_id=f"gcs_2_sql_{table}",
                              bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}  --cluster="sqoop-gcp-outbound-adw-pii"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- export -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --table "{{ params.table }}" --export-dir "gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}" -m 9 -input-fields-terminated-by "|" --columns  {{ params.fields }} ',
                              dag=dag,
                              params= {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                       'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                       'environment': Variable.get("environment"),
                                       'Cdl_Server':Variable.get("Cdl_Server"),
                                       'Cdl_db': Variable.get("Cdl_db"),
                                       'table': table,
                                       'fields': fields})

    audit_complete_date = BashOperator(task_id=f"audit_complete_date_{table}",
                                    bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-adw-pii"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "update dbo.cdl_load_status  set [load_complete_dtm] = CURRENT_TIMESTAMP where table_name = '"'"'{{ params.table }}'"'"' "    ',
                                    dag=dag,
                                    params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                            'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                            'environment': Variable.get("environment"),
                                            'Cdl_Server': Variable.get("Cdl_Server"),
                                            'Cdl_db': Variable.get("Cdl_db"),
                                            'table': table,
                                            'fields': fields})

    cleanup_work_source >> prepare_work_source >> bq_2_gcs >> create_cluster >> audit_start_date >> setup_sql >> gcs_2_sql >> audit_complete_date >> delete_cluster

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([delete_cluster])