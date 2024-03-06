from datetime import datetime, timedelta, date
from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = 'integrated_rap'

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

query_insert_to_integration = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.adw.rap_service_call_payments` (
    tran_typ_cd,
    service_call_dt,
    rap_call_id,
    contact_adw_key,
    service_club_cd,
    rap_program_cd,
    service_call_tow_miles,
    total_payment_amt,
    update_dt,
    integrate_insert_datetime,
    integrate_insert_batch_number
)
select  a.TransType,
    a.ServiceDate,
    a.RAPCallID,
    '-1',
    a.ServiceClub,
    a.MemberClub,
    a.TowMiles,
    a.TotalAmount,
    a.UpdateDate,
    CURRENT_DATETIME() as integrate_insert_datetime,
    {{{{ dag_run.id }}}} as integrate_insert_batch_number
from `{iu.INGESTION_PROJECT}.misc.natl_rap_file` a
left join `{int_u.INTEGRATION_PROJECT}.adw.rap_service_call_payments` b on b.tran_typ_cd = a.TransType and b.service_call_dt = a.ServiceDate and b.rap_call_id = a.RAPCallID 
where a.TransType = 'PDC'
and b.service_call_dt is null;
"""

query_match_to_contact = f"""
update `{int_u.INTEGRATION_PROJECT}.adw.rap_service_call_payments` as r
set contact_adw_key = c.contact_adw_key 
from (select  sc_dt, sc_call_mbr_id,sc_cntc_lst_nm,sc_cntc_fst_nm,bl_tel_nr_ext, row_number() over(partition by sc_dt, sc_call_mbr_id order by adw_lake_insert_datetime desc) as sequence 
              from `{iu.INGESTION_PROJECT}.d3.arch_call`) AS a 
inner join (select contact_adw_key, contact_first_nm, contact_last_nm, contact_phone_nbr, row_number() over(partition by contact_first_nm, contact_last_nm, contact_phone_nbr order by integrate_insert_datetime) as sequence 
              from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info` where length(contact_phone_nbr)=10) as c on c.contact_first_nm=a.sc_cntc_fst_nm and c.contact_last_nm=a.sc_cntc_lst_nm 
              and c.contact_phone_nbr=a.bl_tel_nr_ext and c.sequence = 1
where a.sequence = 1
and r.service_call_dt = DATE(cast(a.sc_dt as datetime)) 
and r.rap_call_id = a.sc_call_mbr_id
and r.contact_adw_key = '-1';
"""

# DAG Tasks
with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None, catchup=False, concurrency=6, max_active_runs=1) as dag:

    task_insert_to_integration = int_u.run_query('insert_to_integration', query_insert_to_integration)

    task_match_to_contact = int_u.run_query('match_to_contact', query_match_to_contact)

    #Task Sequence

    task_insert_to_integration >> task_match_to_contact

AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([task_match_to_contact])