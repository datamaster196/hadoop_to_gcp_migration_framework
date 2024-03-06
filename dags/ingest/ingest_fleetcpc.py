#######################################################################################################################
##  DAG_NAME :  ingest_fleetcpc
##  PURPOSE :   DAG to load data from source to ingest for fleet CPC
##  PREREQUISITE DAGs : ingest_eds
#######################################################################################################################

from datetime import datetime, timedelta, date
from airflow import DAG
from utils import ingestion_utilities as iu
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = 'ingest_fleetcpc'
SCHEDULE = Variable.get("ingest_fleetcpc_schedule", default_var='') or None

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

def run_query(task_id, query):
    task = BigQueryOperator(task_id=task_id, sql=query, use_legacy_sql=False, retries=0)
    return task

# Define SQL Queries
query_create_work_tables = f"""
CREATE OR REPLACE TABLE `{iu.INGESTION_PROJECT}.work.fleet_cpc_work` (
	Year		INT64,
	Month		INT64,
	FacilityID  STRING,
	CPC         NUMERIC,
	Location    STRING,
);
CREATE OR REPLACE TABLE `{iu.INGESTION_PROJECT}.work.fleet_cpc_work_cost` (
	Year		INT64 NOT NULL,
	Month		INT64 NOT NULL,
	FacilityID  STRING NOT NULL,
	CallCost    NUMERIC NOT NULL
);
CREATE OR REPLACE TABLE `{iu.INGESTION_PROJECT}.work.fleet_cpc_stage` (
	Year		INT64 NOT NULL,
	Month		INT64 NOT NULL,
	FacilityID  STRING NOT NULL,
	CPC         NUMERIC NOT NULL,
	Location    STRING NOT NULL,
	Factor		NUMERIC NOT NULL
);
"""

query_insert_cpc_work_cost = f"""
INSERT INTO `{iu.INGESTION_PROJECT}.work.fleet_cpc_work_cost` (Year, Month, FacilityID, CallCost)
SELECT	EXTRACT(YEAR FROM a.arch_date) as Year,
		EXTRACT(MONTH FROM a.arch_date) as Month,
		sf.svc_facl_id,
		a.call_cost 
FROM (SELECT CAST(arch_date as DATETIME) as arch_date, CAST(call_cost as NUMERIC) as call_cost, facil_int_id, status_cd, call_source, chg_entitlement, 
        row_number() over(partition by comm_ctr_id, sc_dt, sc_id order by adw_lake_insert_datetime desc) as sequence 
        FROM `{iu.INGESTION_PROJECT}.d3.arch_call`) AS a
INNER JOIN	 ( 
		SELECT DISTINCT facil_int_id, svc_facl_id	
		FROM  `{iu.INGESTION_PROJECT}.d3.service_facility`
		WHERE SVC_FACL_ID in (SELECT distinct FacilityID FROM `{iu.INGESTION_PROJECT}.work.fleet_cpc_work`)
) AS sf ON sf.facil_int_id = a.facil_int_id
WHERE   a.arch_date >= (SELECT min(DATETIME(Year, Month, 1, 0, 0, 0)) FROM `{iu.INGESTION_PROJECT}.work.fleet_cpc_work`)
AND		IFNULL(a.status_cd,'') != 'KI'
AND		a.call_cost > 0 		
AND NOT (IFNULL(a.call_source,'') = 'REM' and IFNULL(a.chg_entitlement,'') = 'N') 
AND EXISTS (SELECT 1 FROM  `{iu.INGESTION_PROJECT}.work.fleet_cpc_work` c
		WHERE c.FacilityID = sf.SVC_FACL_ID AND c.Year = EXTRACT(year from a.ARCH_DATE) AND c.Month = EXTRACT(month from a.ARCH_DATE)
)
AND a.sequence = 1;
"""

query_calc_factor_insert_stage = f"""
insert into `{iu.INGESTION_PROJECT}.work.fleet_cpc_stage` (Year, Month, FacilityID, CPC, Location, Factor)
select c.Year, c.Month, c.FacilityID, c.CPC, c.Location, IFNULL(round(d.TotCPC/d.TotCost, 6), 0) as Factor
from `{iu.INGESTION_PROJECT}.work.fleet_cpc_work` c
left join 
    (select a.Year, a.Month, a.Location, sum(a.CPC) as TotCPC, sum(b.CallCost) as TotCost 
    from `{iu.INGESTION_PROJECT}.work.fleet_cpc_work` a
    inner join `{iu.INGESTION_PROJECT}.work.fleet_cpc_work_cost` b on b.Year = a.Year and b.Month = a.Month and b.FacilityID = a.FacilityID
    Group By a.Year, a.Month, a.Location
) d on d.Year = c.Year and d.Month = c.Month and d.Location = c.Location;
"""

query_update_fleet_cpc = f"""
UPDATE `{iu.INGESTION_PROJECT}.misc.fleet_cpc` as a
SET CPC = b.CPC,
    Factor = b.Factor
FROM `{iu.INGESTION_PROJECT}.work.fleet_cpc_stage` b 
WHERE b.Year = a.Year and b.Month = a.Month and b.FacilityID = a.FacilityID
AND (a.CPC <> b.CPC OR a.Factor <> b.Factor);
"""

query_insert_fleet_cpc = f"""
INSERT INTO `{iu.INGESTION_PROJECT}.misc.fleet_cpc` (Year, Month, FacilityID, CPC, Location, Factor, adw_lake_insert_datetime, adw_lake_batch_number)
SELECT a.Year, a.Month, a.FacilityID, a.CPC, a.Location, a.Factor, CURRENT_DATETIME(), {{{{ dag_run.id }}}}  
FROM `{iu.INGESTION_PROJECT}.work.fleet_cpc_stage` a
LEFT JOIN `{iu.INGESTION_PROJECT}.misc.fleet_cpc` b on b.Year = a.Year and b.Month = a.Month and b.FacilityID = a.FacilityID
WHERE b.Year IS NULL;
"""

# DAG Tasks
with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False) as dag:

    task_create_work_tables = run_query('create_work_tables', query_create_work_tables)

    task_load_files = BashOperator(task_id='load_files',
                                   bash_command='bq load --project_id={{ var.value.INGESTION_PROJECT }} --source_format=CSV --autodetect --replace work.fleet_cpc_work gs://goanywhere_file_repo/{{ var.value.environment }}/FleetCPC/*.csv Year:INT64,Month:INT64,FacilityID:STRING,CPC:NUMERIC,Location:STRING',
                                   dag=dag)

    task_insert_cpc_work_cost = run_query('insert_cpc_work_cost', query_insert_cpc_work_cost)

    task_calc_factor_insert_stage = run_query('calc_factor_insert_stage', query_calc_factor_insert_stage)

    task_update_fleet_cpc = run_query('update_fleet_cpc', query_update_fleet_cpc)

    task_insert_fleet_cpc = run_query('insert_fleet_cpc', query_insert_fleet_cpc)

    task_archive = BashOperator(task_id='archive_files',
                                bash_command='gsutil -q stat gs://goanywhere_file_repo/{{ var.value.environment }}/FleetCPC/*.csv; if [ $? == 0 ]; then gsutil mv gs://goanywhere_file_repo/{{ var.value.environment }}/FleetCPC/*.csv gs://goanywhere_file_repo/{{ var.value.environment }}/FleetCPC/Archive/; fi',
                                dag=dag)

    #Task Sequence

    task_create_work_tables >> task_load_files >> task_insert_cpc_work_cost >> task_calc_factor_insert_stage >> task_update_fleet_cpc >> task_insert_fleet_cpc >> task_archive


AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INGESTION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream(task_archive)