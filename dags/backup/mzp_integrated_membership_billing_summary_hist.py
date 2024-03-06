from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_billing_summary_hist"

default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'start_date'      : datetime(2019, 8, 22),
    'email'           : ['airflow@example.com'],
    'catchup'         : False,
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=5),
}


####################################################################################################################################
# Billing_Summary_hist
####################################################################################################################################
######################################################################
# Load Billing_Summary Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################




membership_billing_summary_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_source` AS
  SELECT
 billing_summary_source.bill_summary_ky as bill_summary_source_key,
 billing_summary_source.membership_ky ,
 billing_summary_source.process_dt as bill_process_date,
 billing_summary_source.notice_nr as bill_notice_number,
 billing_summary_source.bill_at as bill_amount,
 billing_summary_source.credit_at as bill_credit_amount,
 billing_summary_source.bill_type,
 billing_summary_source.expiration_dt as member_expiration_date,
 billing_summary_source.payment_at as bill_paid_amount,
 billing_summary_source.renew_method_cd as bill_renewal_method,
 billing_summary_source.bill_panel as bill_panel_code,
 billing_summary_source.ebill_fl as bill_ebilling_indicator,
 billing_summary_source.last_upd_dt,
 ROW_NUMBER() OVER(PARTITION BY billing_summary_source.bill_summary_ky ORDER BY billing_summary_source.last_upd_dt DESC) AS dupe_check
   FROM
   `{iu.INGESTION_PROJECT}.mzp.bill_summary` AS billing_summary_source
 WHERE
  billing_summary_source.last_upd_dt IS NOT NULL

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_billing_summary_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` AS
 SELECT
 adw_memship.membership_adw_key as membership_adw_key,
 billing_summary_source.bill_summary_source_key,
 billing_summary_source.bill_process_date,
 billing_summary_source.bill_notice_number,
 billing_summary_source.bill_amount,
 billing_summary_source.bill_credit_amount,
 billing_summary_source.bill_type,
 billing_summary_source.member_expiration_date,
 billing_summary_source.bill_paid_amount,
 billing_summary_source.bill_renewal_method,
 billing_summary_source.bill_panel_code,
 billing_summary_source.bill_ebilling_indicator,
 CAST (billing_summary_source.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(adw_memship.membership_adw_key,''),'|',
ifnull(billing_summary_source.bill_summary_source_key,''),'|',
ifnull(billing_summary_source.bill_process_date,''),'|',
ifnull(billing_summary_source.bill_notice_number,''),'|',
ifnull(billing_summary_source.bill_amount,''),'|',
ifnull(billing_summary_source.bill_credit_amount,''),'|',
ifnull(billing_summary_source.bill_type,''),'|',
ifnull(billing_summary_source.member_expiration_date,''),'|',
ifnull(billing_summary_source.bill_paid_amount,''),'|',
ifnull(billing_summary_source.bill_renewal_method,''),'|',
ifnull(billing_summary_source.bill_panel_code,''),'|',
ifnull(billing_summary_source.bill_ebilling_indicator,''),'|'
))) as adw_row_hash
 
FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_source` AS billing_summary_source
    LEFT JOIN
     `{int_u.INTEGRATION_PROJECT}.member.dim_membership` adw_memship
 on billing_summary_source.membership_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING) AND active_indicator='Y'
 where dupe_check = 1
 

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_summary_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_type_2_hist` AS
WITH
  membership_billing_summary_hist AS (
  SELECT
    bill_summary_source_key ,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY bill_summary_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY bill_summary_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    bill_summary_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    membership_billing_summary_hist 
  ), set_groups AS (
  SELECT
    bill_summary_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY bill_summary_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    bill_summary_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    bill_summary_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  bill_summary_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_billing_summary
membership_billing_summary_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary`
  (
membership_billing_summary_adw_key,
membership_adw_key,
bill_summary_source_key,
bill_process_date,
bill_notice_number,
bill_amount,
bill_credit_amount,
bill_type,
member_expiration_date,
bill_paid_amount,
bill_renewal_method,
bill_panel_code,
bill_ebilling_indicator,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
  ) 
SELECT
membership_billing_summary_adw_key,
COALESCE(source.membership_adw_key,'-1') ,
SAFE_CAST(source.bill_summary_source_key AS INT64),
CAST(substr (source.bill_process_date,0,10) AS Date),
CAST(source.bill_notice_number AS INT64),
CAST(source.bill_amount AS NUMERIC),
CAST(source.bill_credit_amount AS NUMERIC),
source.bill_type,
CAST(substr (source.member_expiration_date,0,10) AS Date),
CAST(source.bill_paid_amount AS NUMERIC),
source.bill_renewal_method,
source.bill_panel_code,
source.bill_ebilling_indicator,
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
( CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END) ,
source.adw_row_hash,
CURRENT_DATETIME() ,
1 ,
CURRENT_DATETIME() ,
1 
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` source
  ON (
  source.bill_summary_source_key=hist_type_2.bill_summary_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_billing_summary_adw_key,
    bill_summary_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed`
  GROUP BY
    bill_summary_source_key) pk
ON (source.bill_summary_source_key=pk.bill_summary_source_key)
  

"""


with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS
    
    # membership_billing_summary
    
    # membership_billing_summary - Workarea loads
    task_membership_billing_summary_work_source = int_u.run_query("build_membership_billing_summary_work_source", membership_billing_summary_work_source)
    task_membership_billing_summary_work_transformed = int_u.run_query("build_membership_billing_summary_work_transformed", membership_billing_summary_work_transformed)
    task_membership_billing_summary_work_type2_logic = int_u.run_query("build_membership_billing_summary_work_type2_logic", membership_billing_summary_work_type2_logic)
    
    # membership_billing_summary - Merge Load
    task_membership_billing_summary_insert = int_u.run_query('merge_membership_billing_summary', membership_billing_summary_insert)
    
    
    # membership_billing_summary    
     

    task_membership_billing_summary_work_source >> task_membership_billing_summary_work_transformed
    task_membership_billing_summary_work_transformed >> task_membership_billing_summary_work_type2_logic
    
    task_membership_billing_summary_work_type2_logic >> task_membership_billing_summary_insert