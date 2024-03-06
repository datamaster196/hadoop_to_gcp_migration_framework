from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_mzp_membership_gl_payments_applied"

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



######################################################################
# Load membership_gl_payments_applied Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_gl_payments_applied_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
   `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_source` AS
 SELECT
 membership_gl_pay_app_source.JOURNAL_ENTRY_KY ,
 membership_gl_pay_app_source.GL_ACCOUNT_KY ,
 membership_gl_pay_app_source.membership_ky,
 membership_gl_pay_app_source.POST_DT ,
 membership_gl_pay_app_source.PROCESS_DT ,
 membership_gl_pay_app_source.Journal_at ,
 membership_gl_pay_app_source.CREATE_DT ,
 membership_gl_pay_app_source.DR_CR_IND ,
 membership_gl_pay_app_source.last_upd_dt, 

   ROW_NUMBER() OVER(PARTITION BY membership_gl_pay_app_source.JOURNAL_ENTRY_KY ORDER BY membership_gl_pay_app_source.last_upd_dt DESC) AS dupe_check
 FROM
   `{iu.INGESTION_PROJECT}.mzp.journal_entry` AS membership_gl_pay_app_source
  WHERE
    CAST(membership_gl_pay_app_source.last_upd_dt AS datetime) > (
    SELECT
     MAX(effective_start_datetime)
    FROM
     `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_gl_payments_applied_work_transformed = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` AS
SELECT
  COALESCE(membership.membership_adw_key,
    "-1") AS membership_adw_key,
  COALESCE(office.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  CAST(membership_gl_pay_app_source.JOURNAL_ENTRY_KY AS INT64) AS gl_payment_journal_source_key,
  gl_account.GL_DESCR AS gl_payment_gl_description,
  gl_account.GL_ACCT_NUMBER AS gl_payment_account_number,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.POST_DT,1,10)) AS Date) AS gl_payment_post_date,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.PROCESS_DT,1,10)) AS Date) AS gl_payment_process_date,
  SAFE_CAST(membership_gl_pay_app_source.Journal_at AS NUMERIC) AS gl_payment_journal_amount,
  SAFE_CAST(membership_gl_pay_app_source.CREATE_DT AS DATETIME) AS gl_payment_journal_created_datetime,
  membership_gl_pay_app_source.DR_CR_IND AS gl_payment_journal_debit_credit_indicator,
  membership_gl_pay_app_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(membership.membership_adw_key,
            "-1"),
          ''),'|', ifnull(COALESCE(office.aca_office_adw_key,
            "-1"),
          ''),'|', ifnull(gl_account.GL_DESCR,
          ''),'|', ifnull(gl_account.GL_ACCT_NUMBER,
          ''),'|',ifnull(membership_gl_pay_app_source.POST_DT,
          ''),'|',ifnull(membership_gl_pay_app_source.PROCESS_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.Journal_at,
          ''), '|',ifnull(membership_gl_pay_app_source.CREATE_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.DR_CR_IND,
          '')))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_source` AS membership_gl_pay_app_source
LEFT OUTER JOIN (
  SELECT
    membership_adw_key,
    membership_source_system_key,
    active_indicator
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership`
  WHERE
    active_indicator='Y' ) membership
ON
  CAST(membership.membership_source_system_key AS string)=membership_gl_pay_app_source.JOURNAL_ENTRY_KY
LEFT OUTER JOIN (
  SELECT
    GL_ACCOUNT_KY,
    GL_DESCR,
    GL_ACCT_NUMBER,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY GL_ACCOUNT_KY ORDER BY NULL ) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.gl_account` ) gl_account
ON
  gl_account.GL_ACCOUNT_KY=membership_gl_pay_app_source.GL_ACCOUNT_KY
  AND gl_account.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    BRANCH_CD,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_KY ORDER BY NULL) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.branch` ) branch
ON
  branch.BRANCH_KY=gl_account.BRANCH_KY
  AND branch.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    membership_branch_code
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office`
  WHERE
    active_indicator='Y') office
ON
  office.membership_branch_code=branch.BRANCH_KY
WHERE
  membership_gl_pay_app_source.dupe_check=1
  
"""

######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################
 
membership_gl_payments_applied_work_final_staging = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_stage` AS
SELECT
  COALESCE(target.membership_gl_payments_applied_adw_key,
    GENERATE_UUID()) membership_gl_payments_applied_adw_key,
  source.membership_adw_key,
  source. aca_office_adw_key,
  source. gl_payment_journal_source_key AS gl_payment_journal_source_key,
  source. gl_payment_gl_description,
  source. gl_payment_account_number,
  source. gl_payment_post_date AS gl_payment_post_date,
  source. gl_payment_process_date  AS gl_payment_process_date,
  source. gl_payment_journal_amount  AS gl_payment_journal_amount,
  source. gl_payment_journal_created_datetime  AS gl_payment_journal_created_datetime,
  source. gl_payment_journal_debit_credit_indicator AS gl_payment_journal_debit_credit_indicator,
  SAFE_CAST(source.effective_start_datetime AS DATETIME) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  1 integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  1 integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied` target
ON
  (source.gl_payment_journal_source_key=target.gl_payment_journal_source_key
    AND target.active_indicator='Y')
WHERE
  target.gl_payment_journal_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.membership_gl_payments_applied_adw_key,
  target.membership_adw_key,
  target.aca_office_adw_key,
  target.gl_payment_journal_source_key,
  target.gl_payment_gl_description,
  target.gl_payment_account_number,
  target.gl_payment_post_date,
  target.gl_payment_process_date,
  target.gl_payment_journal_amount,
  target.gl_payment_journal_created_datetime,
  target.gl_payment_journal_debit_credit_indicator,
  target.effective_start_datetime,
  DATETIME_SUB(CAST(source.effective_start_datetime AS datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied` target
ON
  (source. gl_payment_journal_source_key =target. gl_payment_journal_source_key
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_gl_payments_applied
membership_gl_payments_applied_merge = f"""
 
 MERGE INTO
   `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied` a
 USING
   `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_stage` b
 ON
   (a.gl_payment_journal_source_key = b.gl_payment_journal_source_key
   AND a.effective_start_datetime = b.effective_start_datetime)
   WHEN NOT MATCHED THEN INSERT ( 
   membership_gl_payments_applied_adw_key , 
   membership_adw_key , 
   aca_office_adw_key , 
   gl_payment_journal_source_key , 
   gl_payment_gl_description , 
   gl_payment_account_number , 
   gl_payment_post_date , 
   gl_payment_process_date ,
   gl_payment_journal_amount ,
   gl_payment_journal_created_datetime ,
   gl_payment_journal_debit_credit_indicator ,
   effective_start_datetime, 
   effective_end_datetime, 
   active_indicator, 
   adw_row_hash, 
   integrate_insert_datetime, 
   integrate_insert_batch_number, 
   integrate_update_datetime, 
   integrate_update_batch_number ) VALUES (
   b.membership_gl_payments_applied_adw_key , 
   b.membership_adw_key , 
   b.aca_office_adw_key , 
   b.gl_payment_journal_source_key , 
   b.gl_payment_gl_description , 
   b.gl_payment_account_number , 
   b.gl_payment_post_date , 
   b.gl_payment_process_date ,
   b.gl_payment_journal_amount ,
   b.gl_payment_journal_created_datetime ,
   b.gl_payment_journal_debit_credit_indicator ,
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

    # membership_gl_payments_applied

    # membership_gl_payments_applied - Workarea loads
    task_membership_gl_payments_applied_work_source = int_u.run_query("build_membership_gl_payments_applied_work_source", membership_gl_payments_applied_work_source)
    task_membership_gl_payments_applied_work_transformed = int_u.run_query("build_membership_gl_payments_applied_work_transformed", membership_gl_payments_applied_work_transformed)
    task_membership_gl_payments_applied_work_final_staging = int_u.run_query("build_membership_gl_payments_applied_work_final_staging",
                                                         membership_gl_payments_applied_work_final_staging)

    # membership_gl_payments_applied - Merge Load
    task_membership_gl_payments_applied_merge = int_u.run_query('merge_membership_gl_payments_applied', membership_gl_payments_applied_merge)


    ######################################################################
    # DEPENDENCIES



    # membership_gl_payments_applied

    task_membership_gl_payments_applied_work_source >> task_membership_gl_payments_applied_work_transformed
    task_membership_gl_payments_applied_work_transformed >> task_membership_gl_payments_applied_work_final_staging

    task_membership_gl_payments_applied_work_final_staging >> task_membership_gl_payments_applied_merge
    

