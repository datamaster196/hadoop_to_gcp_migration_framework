#######################################################################################################################
##  DAG_NAME :  ingest_rap_file
##  PURPOSE :   DAG to load data from source to ingest for RAP file
##  PREREQUISITE DAGs : ingest_eds
#######################################################################################################################

from datetime import datetime, timedelta, date
from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = 'ingest_rap_file'
SCHEDULE = Variable.get("ingest_rap_file_schedule", default_var='') or None

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
        'dag_name': DAG_TITLE
    }
}

# Define SQL Queries
query_drop_work_tables = f"""
DROP TABLE IF EXISTS `{iu.INGESTION_PROJECT}.work.rap_1_work`;
CREATE OR REPLACE TABLE `{iu.INGESTION_PROJECT}.work.rap_2_work` (record STRING, source_file STRING); 
DROP TABLE IF EXISTS `{iu.INGESTION_PROJECT}.work.rap_stage`;
"""

query_create_rap_stage = f"""
CREATE OR REPLACE TABLE `{iu.INGESTION_PROJECT}.work.rap_stage` (
	TransType						STRING,
	SubmitClub						STRING,
	ProcessorID						STRING,
	ServiceClub						STRING,
	ServiceDate						DATE NOT NULL,
	ServiceID						STRING,
	RAPCallID						STRING NOT NULL,
	FacilityID						STRING,
	ProblemCode						STRING,
	MedDutyInd						STRING,
	MemberClub						STRING,
	MemberID						STRING,
	MemberType						STRING,
	ExpiryDate						DATE,
	LastName						STRING,
	FirstName						STRING,
	VIN							    STRING,
	Odometer						STRING,
	EnrouteMiles					NUMERIC,
	TowMiles						NUMERIC,
	TaxAmount						NUMERIC,
	TotalAmount						NUMERIC,
	DisputeCode						STRING,
	ErrorBuffer						STRING,
	CostBuffer						STRING,
	SourceFile						STRING NOT NULL,
	UpdateDate						DATE NOT NULL,
	ID							    STRING
)
"""

query_insert_into_rap_stage_1 = f"""
CREATE TEMP FUNCTION getUpdateDate(s STRING) AS (
  date_add(cast(concat(substr(s,5,4),'-',substr(s,3,2),'-05') as date), interval 1 month)
);
CREATE TEMP FUNCTION getDate(s STRING) AS (
  if ((trim(s) = ''), NULL, cast(concat(substr(s,5,4),'-',substr(s,1,2),'-',substr(s,3,2)) as date))
);
INSERT INTO `{iu.INGESTION_PROJECT}.work.rap_stage` (
    TransType,
    SubmitClub,
    ProcessorID,
    ServiceClub,
    ServiceDate,
    ServiceID,
    RAPCallID,
    FacilityID,
    ProblemCode,
    MedDutyInd,
    MemberClub,
    MemberID,
    MemberType,
    ExpiryDate,
    LastName,
    FirstName,
    VIN,
    Odometer,
    EnrouteMiles,
    TowMiles,
    TaxAmount,
    TotalAmount,
    DisputeCode,
    ErrorBuffer,
    CostBuffer,
    SourceFile, 
    UpdateDate,
    ID
)
SELECT TransType,
    SubmitClub,
    ProcessorID,
    ServiceClub,
    getDate(ServiceDate) as ServiceDate,
    ServiceID,
    RAPCallID,
    FacilityID,
    ProblemCode,
    MedDutyInd,
    MemberClub,
    MemberID,
    MemberType,
    getDate(ExpiryDate) as ExpiryDate,
    LastName,
    FirstName,
    VIN,
    Odometer,
    cast(EnrouteMiles as NUMERIC) as EnrouteMiles,
    cast(TowMiles as NUMERIC) as TowMiles,
    cast(TaxAmount as NUMERIC) / 100 as TaxAmount,
    cast(TotalAmount as NUMERIC) / 100 as TotalAmount,
    DisputeCode,
    ErrorBuffer,
    CostBuffer,
    substr(source_file,strpos(source_file,'/RP')+1) as file_name, 
    getUpdateDate(substr(source_file,strpos(source_file,'/RP')+1)) as UpdateDate,
    GENERATE_UUID()
    FROM `{iu.INGESTION_PROJECT}.work.rap_1_work`
"""

query_insert_into_rap_stage_2 = f"""
CREATE TEMP FUNCTION getUpdateDate(s STRING) AS (
  date_add(cast(concat(substr(s,5,4),'-',substr(s,3,2),'-05') as date), interval 1 month)
);
CREATE TEMP FUNCTION getDate(s STRING) AS (
  if ((trim(s) = ''), NULL, cast(concat(substr(s,5,4),'-',substr(s,1,2),'-',substr(s,3,2)) as date))
);
INSERT INTO `{iu.INGESTION_PROJECT}.work.rap_stage` (
    TransType,
    SubmitClub,
    ProcessorID,
    ServiceClub,
    ServiceDate,
    ServiceID,
    RAPCallID,
    FacilityID,
    ProblemCode,
    MedDutyInd,
    MemberClub,
    MemberID,
    MemberType,
    ExpiryDate,
    LastName,
    FirstName,
    VIN,
    Odometer,
    EnrouteMiles,
    TowMiles,
    TaxAmount,
    TotalAmount,
    DisputeCode,
    ErrorBuffer,
    SourceFile, 
    UpdateDate,
    ID
)
SELECT  substr(record,1,3) as TransType,
        substr(record,4,3) as SubmitClub,
        substr(record,7,3) as ProcessorID,
        substr(record,10,3) as ServiceClub,
        getDate(substr(record,13,8)) as ServiceDate,
        substr(record,21,8) as ServiceID,
        substr(record,29,8) as RAPCallID,
        rtrim(substr(record,37,5)) as FacilityID,
        substr(record,42,2) as ProblemCode,
        substr(record,44,1) as MedDutyInd,
        substr(record,45,3) as MemberClub,
        rtrim(substr(record,48,25)) as MemberID,
        rtrim(substr(record,73,4)) as MemberType,
        getDate(substr(record,77,8)) as ExpiryDate,
        rtrim(substr(record,85,20)) as LastName,
        rtrim(substr(record,105,15)) as FirstName,
        rtrim(substr(record,120,17)) as VIN,
        rtrim(substr(record,137,7)) as Odometer,
        CAST(substr(record,144,3) as NUMERIC) as EnrouteMiles,
        CAST(substr(record,147,3) as NUMERIC) as TowMiles,
        CAST(substr(record,150,6) as NUMERIC) / 100 as TaxAmount,
        CAST(substr(record,156,6) as NUMERIC) / 100 TotalAmount,
        substr(record,162,2) as DisputeCode,
        rtrim(substr(record,164,190)) as ErrorBuffer,
        substr(source_file,strpos(source_file,'/RP')+1) as SourceFile, 
        getUpdateDate(substr(source_file,strpos(source_file,'/RP')+1)) as UpdateDate,
	    GENERATE_UUID()
FROM `{iu.INGESTION_PROJECT}.work.rap_2_work`
"""

query_dedupe_rap_stage = f"""
DELETE FROM `{iu.INGESTION_PROJECT}.work.rap_stage` 
WHERE ID IN 
(select ID FROM
  (SELECT ID, ROW_NUMBER() OVER (PARTITION BY TransType, ServiceDate, RAPCallID ORDER BY ServiceID DESC) as num
  FROM `{iu.INGESTION_PROJECT}.work.rap_stage`
  )
where num > 1
)
"""

query_insert_to_lake = f"""
INSERT INTO `{iu.INGESTION_PROJECT}.misc.natl_rap_file` (
    TransType,
    SubmitClub,
    ProcessorID,
    ServiceClub,
    ServiceDate,
    ServiceID,
    RAPCallID,
    FacilityID,
    ProblemCode,
    MedDutyInd,
    MemberClub,
    MemberID,
    MemberType,
    ExpiryDate,
    LastName,
    FirstName,
    VIN,
    Odometer,
    EnrouteMiles,
    TowMiles,
    TaxAmount,
    TotalAmount,
    DisputeCode,
    ErrorBuffer,
    CostBuffer,
    SourceFile, 
    UpdateDate,
    adw_lake_insert_datetime,
    adw_lake_batch_number
)
select a.TransType,
    a.SubmitClub,
    a.ProcessorID,
    a.ServiceClub,
    a.ServiceDate,
    a.ServiceID,
    a.RAPCallID,
    a.FacilityID,
    a.ProblemCode,
    a.MedDutyInd,
    a.MemberClub,
    a.MemberID,
    a.MemberType,
    a.ExpiryDate,
    a.LastName,
    a.FirstName,
    a.VIN,
    a.Odometer,
    a.EnrouteMiles,
    a.TowMiles,
    a.TaxAmount,
    a.TotalAmount,
    a.DisputeCode,
    a.ErrorBuffer,
    a.CostBuffer,
    a.SourceFile, 
    a.UpdateDate,
    CURRENT_DATETIME(),
    {{{{ dag_run.id }}}}
from `{iu.INGESTION_PROJECT}.work.rap_stage` a
left join `{iu.INGESTION_PROJECT}.misc.natl_rap_file` b on b.TransType = a.TransType and b.ServiceDate = a.ServiceDate and b.RAPCallID = a.RAPCallID 
where b.ServiceDate is null;
"""

# DAG Tasks
with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6, max_active_runs=1) as dag:

    # dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6)

    task_drop_work_tables = int_u.run_query('drop_work_tables', query_drop_work_tables)

    task_repo_to_lake_1 = BashOperator(task_id='move_from_repo_1',
                                   bash_command='gsutil -q stat gs://goanywhere_file_repo/{{ var.value.environment }}/RAP/RP*.241; if [ $? == 0 ]; then gsutil mv gs://goanywhere_file_repo/{{ var.value.environment }}/RAP/RP*.241 gs://adw-lake-{{ var.value.environment }}-central/RAP_2/; fi',
                                   dag=dag)

    task_repo_to_lake_2 = BashOperator(task_id='move_from_repo_2',
                                   bash_command='gsutil mv gs://goanywhere_file_repo/{{ var.value.environment }}/RAP/RP* gs://adw-lake-{{ var.value.environment }}-central/RAP_1/',
                                   dag=dag)

    task_load_to_bq_work_1 = BashOperator(task_id='load_to_work_1',
                                   bash_command='bq query --project_id={{ var.value.INGESTION_PROJECT }} --destination_table work.rap_1_work --replace --external_table_definition=rap1work::TransType:STRING,SubmitClub:STRING,ProcessorID:STRING,ServiceClub:STRING,ServiceDate:STRING,ServiceID:STRING,RAPCallID:STRING,FacilityID:STRING,ProblemCode:STRING,MedDutyInd:STRING,MemberClub:STRING,MemberID:STRING,MemberType:STRING,ExpiryDate:STRING,LastName:STRING,FirstName:STRING,VIN:STRING,Odometer:STRING,EnrouteMiles:STRING,TowMiles:STRING,TaxAmount:STRING,TotalAmount:STRING,DisputeCode:STRING,ErrorBuffer:STRING,CostBuffer:STRING@CSV=gs://adw-lake-{{ var.value.environment }}-central/RAP_1/RP* ''SELECT TransType,SubmitClub,ProcessorID,ServiceClub,ServiceDate,ServiceID,RAPCallID,FacilityID,ProblemCode,MedDutyInd,MemberClub,MemberID,MemberType,ExpiryDate,LastName,FirstName,VIN,Odometer,EnrouteMiles,TowMiles,TaxAmount,TotalAmount,DisputeCode,ErrorBuffer,CostBuffer, _FILE_NAME as source_file FROM rap1work''',
                                   dag=dag)

    task_load_to_bq_work_2 = BashOperator(task_id='load_to_work_2',
                                   bash_command='gsutil -q stat gs://adw-lake-{{ var.value.environment }}-central/RAP_2/RP*; if [ $? == 0 ]; then bq query --project_id={{ var.value.INGESTION_PROJECT }} --destination_table work.rap_2_work --replace --external_table_definition=rap2work::record:STRING@CSV=gs://adw-lake-{{ var.value.environment }}-central/RAP_2/RP* ''SELECT record, _FILE_NAME as source_file FROM rap2work''; fi',
                                   dag=dag)

    task_create_rap_stage = int_u.run_query('create_stage', query_create_rap_stage)

    task_insert_into_rap_stage_1 = int_u.run_query('insert_stage_1', query_insert_into_rap_stage_1)

    task_insert_into_rap_stage_2 = int_u.run_query('insert_stage_2', query_insert_into_rap_stage_2)

    task_dedupe_rap_stage = int_u.run_query('dedupe_stage', query_dedupe_rap_stage)

    task_insert_to_lake = int_u.run_query('insert_to_lake', query_insert_to_lake)

    task_archive_1 = BashOperator(task_id='archive_files_1',
                                   bash_command='gsutil -q stat gs://adw-lake-{{ var.value.environment }}-central/RAP_1/RP*; if [ $? == 0 ]; then gsutil mv gs://adw-lake-{{ var.value.environment }}-central/RAP_1/RP* gs://adw-lake-{{ var.value.environment }}-central/RAP_1/Archive/; fi',
                                   dag=dag)

    task_archive_2 = BashOperator(task_id='archive_files_2',
                                   bash_command='gsutil -q stat gs://adw-lake-{{ var.value.environment }}-central/RAP_2/RP*; if [ $? == 0 ]; then gsutil mv gs://adw-lake-{{ var.value.environment }}-central/RAP_2/RP* gs://adw-lake-{{ var.value.environment }}-central/RAP_2/Archive/; fi',
                                   dag=dag)

    trigger_dag = TriggerDagRunOperator(
        task_id="integrated_rap",
        trigger_dag_id="integrated_rap",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "integrated_rap has been triggered"},
        dag=dag)

    #Task Sequence

    task_drop_work_tables >> task_repo_to_lake_1 >> task_repo_to_lake_2

    task_repo_to_lake_2 >> task_load_to_bq_work_1 >> task_create_rap_stage
    task_repo_to_lake_2 >> task_load_to_bq_work_2 >> task_create_rap_stage

    task_create_rap_stage >> task_insert_into_rap_stage_1 >> task_dedupe_rap_stage
    task_create_rap_stage >> task_insert_into_rap_stage_2 >> task_dedupe_rap_stage

    task_dedupe_rap_stage >> task_insert_to_lake 

    task_insert_to_lake >> task_archive_1 >> trigger_dag

    task_insert_to_lake >> task_archive_2 >> trigger_dag

AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([trigger_dag])