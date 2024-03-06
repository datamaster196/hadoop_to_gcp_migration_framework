#Initial load, adw_row_has, batch_number, history vs incremental, level 2??, what if Membership_ky is null?
from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_billing_summary"

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
# Billing_Summary
####################################################################################################################################

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
  billing_summary_source.last_upd_dt IS NOT NULL AND
  CAST(billing_summary_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary`)
  

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
 --where dupe_check = 1
 

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_summary_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_final_stage` AS
SELECT
COALESCE(target.membership_billing_summary_adw_key,GENERATE_UUID()) AS membership_billing_summary_adw_key,
source.membership_adw_key AS membership_adw_key,
SAFE_CAST(source.bill_summary_source_key AS INT64) AS bill_summary_source_key,
CAST(substr (source.bill_process_date,0,10) AS Date) AS bill_process_date,
CAST(source.bill_notice_number AS INT64) AS bill_notice_number,
CAST(source.bill_amount AS NUMERIC) AS bill_amount,
CAST(source.bill_credit_amount AS NUMERIC) AS bill_credit_amount,
source.bill_type,
CAST(substr (source.member_expiration_date,0,10) AS Date) AS member_expiration_date,
CAST(source.bill_paid_amount AS NUMERIC) AS bill_paid_amount,
source.bill_renewal_method,
source.bill_panel_code,
source.bill_ebilling_indicator,
source.last_upd_dt AS effective_start_datetime,
CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
      
 FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` target
  ON
    (source.bill_summary_source_key =SAFE_CAST(target.bill_summary_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_billing_summary_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
target.membership_billing_summary_adw_key,
target.membership_adw_key,
target.bill_summary_source_key,
target.bill_process_date,
target.bill_notice_number,
target.bill_amount,
target.bill_credit_amount,
target.bill_type,
target.member_expiration_date,
target.bill_paid_amount,
target.bill_renewal_method,
target.bill_panel_code,
target.bill_ebilling_indicator,
target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` target
  ON
    (source.bill_summary_source_key =SAFE_CAST(target.bill_summary_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# billing_summary
membership_billing_summary_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_final_stage` b
    ON (a.membership_billing_summary_adw_key = b.membership_billing_summary_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
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
    ) VALUES
    (
b.membership_billing_summary_adw_key,
COALESCE(b.membership_adw_key,'-1'),
b.bill_summary_source_key,
b.bill_process_date,
b.bill_notice_number,
b.bill_amount,
b.bill_credit_amount,
b.bill_type,
b.member_expiration_date,
b.bill_paid_amount,
b.bill_renewal_method,
b.bill_panel_code,
b.bill_ebilling_indicator,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number
    )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number

"""


with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    # billing_summary

    # billing_summary - Workarea loads
    task_billing_summary_work_source = int_u.run_query("build_billing_summary_work_source", membership_billing_summary_work_source)
    task_billing_summary_work_transformed = int_u.run_query("build_billing_summary_work_transformed", membership_billing_summary_work_transformed)
    task_billing_summary_work_final_staging = int_u.run_query("build_billing_summary_work_final_staging", membership_billing_summary_work_final_staging)

    # member - Merge Load
    task_billing_summary_merge = int_u.run_query("merge_billing_summary", membership_billing_summary_merge)



    
    ######################################################################
    # DEPENDENCIES

    # billing_summary    
   

    task_billing_summary_work_source >> task_billing_summary_work_transformed
    task_billing_summary_work_transformed >> task_billing_summary_work_final_staging

    task_billing_summary_work_final_staging >> task_billing_summary_merge

    