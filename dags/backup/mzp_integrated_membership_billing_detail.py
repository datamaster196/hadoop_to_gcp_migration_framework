#Initial load, adw_row_has, batch_number, history vs incremental, level 2??, what if Membership_ky is null?
from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_billing_detail"

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
# Billing_detail
####################################################################################################################################
######################################################################
# Load Billing_detail Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

membership_billing_detail_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_source` AS
 select billing_detail_source.bill_detail_ky as bill_detail_source_key,
 billing_detail_source.bill_summary_ky as bill_summary_source_key,
 billing_detail_source.member_ky ,
 billing_detail_source.rider_ky ,
 billing_detail_source.membership_fees_ky,
 billing_detail_source.bill_detail_at as bill_detail_amount,
 billing_detail_source.detail_text as bill_detail_text,
 billing_detail_source.billing_category_cd as rider_billing_category_code,
 billing_detail_source.solicitation_cd as rider_solicitation_code,
 billing_detail_source.last_upd_dt,
 ROW_NUMBER() OVER(PARTITION BY billing_detail_source.bill_detail_ky ORDER BY billing_detail_source.last_upd_dt DESC) AS dupe_check
   FROM
   `{iu.INGESTION_PROJECT}.mzp.bill_detail` AS billing_detail_source
WHERE
  billing_detail_source.last_upd_dt IS NOT NULL AND
  CAST(billing_detail_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail`)
  

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_billing_detail_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` AS
  SELECT
 billing_summary.membership_billing_summary_adw_key,
 billing_summary.membership_adw_key,
 adw_mem.member_adw_key,
 adw_product.product_adw_key,
 adw_membership_fee.membership_fee_adw_key,
 billing_detail_source.bill_detail_source_key,
 billing_detail_source.bill_summary_source_key,
 billing_detail_source.bill_detail_amount,
 billing_summary.bill_process_date, 
 billing_summary.bill_notice_number,
 billing_summary.bill_type,
 billing_detail_source.bill_detail_text,
 billing_detail_source.rider_billing_category_code,
 adw_cx_codes.code_desc as rider_billing_category_description,
 billing_detail_source.rider_solicitation_code,
 CAST (billing_detail_source.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(billing_summary.membership_adw_key,''),'|',
ifnull(adw_mem.member_adw_key,''),'|',
ifnull(adw_membership_fee.membership_fee_adw_key,''),'|',
ifnull(billing_detail_source.bill_detail_source_key,''),'|',
ifnull(billing_detail_source.bill_summary_source_key,''),'|',
ifnull(billing_detail_source.bill_detail_amount,''),'|',
ifnull(CAST(billing_summary.bill_process_date AS STRING),''),'|',
ifnull(CAST(billing_summary.bill_notice_number AS STRING),''),'|',
ifnull(billing_summary.bill_type,''),'|',
ifnull(billing_detail_source.bill_detail_text,''),'|',
ifnull(billing_detail_source.rider_billing_category_code,''),'|',
ifnull(billing_detail_source.rider_solicitation_code,''),'|',
ifnull(adw_cx_codes.code_desc,''),'|',
ifnull(billing_detail_source.rider_solicitation_code,''),'|'
))) as adw_row_hash
 
FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_source` AS billing_detail_source LEFT JOIN 
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` AS billing_summary 
                                on billing_detail_source.bill_summary_source_key =SAFE_CAST(billing_summary.bill_summary_source_key AS STRING) AND active_indicator='Y' LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` adw_mem
                                on billing_detail_source.member_ky =SAFE_CAST(adw_mem.member_source_system_key AS STRING) AND adw_mem.active_indicator='Y' LEFT JOIN
    (SELECT rider_ky, rider_comp_cd , ROW_NUMBER() OVER(PARTITION BY rider_ky ORDER BY last_upd_dt DESC) AS latest_rec
     FROM `{iu.INGESTION_PROJECT}.mzp.rider`) adw_rider
                                on billing_detail_source.rider_ky = adw_rider.rider_ky and adw_rider.latest_rec = 1 LEFT JOIN
    (select a.product_adw_key, a.product_sku_key
     from `{int_u.INTEGRATION_PROJECT}.adw.dim_product` a JOIN `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` b
     on a.product_category_adw_key = b.product_category_adw_key
     where b.product_category_code = 'RCC' 
     AND a.active_indicator='Y'
     AND b.active_indicator='Y') adw_product 
                                on adw_rider.rider_comp_cd = adw_product.product_sku_key LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` adw_membership_fee
                                on billing_detail_source.membership_fees_ky =SAFE_CAST(adw_membership_fee.membership_fee_source_key AS STRING) AND adw_membership_fee.active_indicator='Y'  LEFT JOIN 
    (SELECT code, code_desc, ROW_NUMBER() OVER(PARTITION BY code_ky ORDER BY last_upd_dt DESC) AS latest_rec
     FROM `{iu.INGESTION_PROJECT}.mzp.cx_codes` 
     where code_type = 'BILTYP') adw_cx_codes
                                on billing_detail_source.rider_billing_category_code =adw_cx_codes.code AND adw_cx_codes.latest_rec = 1
 --where dupe_check = 1
 

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_detail_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_final_stage` AS
SELECT
COALESCE(target.membership_billing_detail_adw_key,GENERATE_UUID()) AS membership_billing_detail_adw_key,
source.membership_billing_summary_adw_key,
COALESCE(source.member_adw_key,'-1') as member_adw_key,
COALESCE(source.product_adw_key,'-1') as product_adw_key,
SAFE_CAST(source.bill_detail_source_key AS INT64) as bill_detail_source_key,
SAFE_CAST(source.bill_summary_source_key AS INT64) as bill_summary_source_key,
CAST(source.bill_process_date AS Date) as bill_process_date,
CAST(source.bill_notice_number AS INT64) as bill_notice_number,
source.bill_type,
CAST(source.bill_detail_amount AS NUMERIC) as bill_detail_amount,
source.bill_detail_text,
source.rider_billing_category_code,
source.rider_billing_category_description,
source.rider_solicitation_code,
source.last_upd_dt AS effective_start_datetime,
CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
      
 FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail` target
  ON
    (source.bill_detail_source_key =SAFE_CAST(target.bill_detail_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_billing_detail_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
target.membership_billing_detail_adw_key,
target.membership_billing_summary_adw_key,
target.member_adw_key,
target.product_adw_key,
target.bill_detail_source_key,
target.bill_summary_source_key,
target.bill_process_date,
target.bill_notice_number,
target.bill_type,
target.bill_detail_amount,
target.bill_detail_text,
target.rider_billing_category_code,
target.rider_billing_category_description,
target.rider_solicitation_code,
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
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail` target
  ON
    (source.bill_detail_source_key =SAFE_CAST(target.bill_detail_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# billing_detail
membership_billing_detail_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_final_stage` b
    ON (a.membership_billing_summary_adw_key = b.membership_billing_detail_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
membership_billing_detail_adw_key,
membership_billing_summary_adw_key,
member_adw_key,
product_adw_key,
bill_detail_source_key,
bill_summary_source_key,
bill_process_date,
bill_notice_number,
bill_type,
bill_detail_amount,
bill_detail_text,
rider_billing_category_code,
rider_billing_category_description,
rider_solicitation_code,
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
b.membership_billing_detail_adw_key,
b.membership_billing_summary_adw_key,
COALESCE(b.member_adw_key,'-1'),
COALESCE(b.product_adw_key,'-1'),
b.bill_detail_source_key,
b.bill_summary_source_key,
b.bill_process_date,
b.bill_notice_number,
b.bill_type,
b.bill_detail_amount,
b.bill_detail_text,
b.rider_billing_category_code,
b.rider_billing_category_description,
b.rider_solicitation_code,
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

    # billing_detail

    # billing_detail - Workarea loads
    task_billing_detail_work_source = int_u.run_query("build_billing_detail_work_source", membership_billing_detail_work_source)
    task_billing_detail_work_transformed = int_u.run_query("build_billing_detail_work_transformed", membership_billing_detail_work_transformed)
    task_billing_detail_work_final_staging = int_u.run_query("build_billing_detail_work_final_staging", membership_billing_detail_work_final_staging)

    # member - Merge Load
    task_billing_detail_merge = int_u.run_query("merge_billing_summary", membership_billing_detail_merge)



    
    ######################################################################
    # DEPENDENCIES

    # billing_detail
   

    task_billing_detail_work_source >> task_billing_detail_work_transformed
    task_billing_detail_work_transformed >> task_billing_detail_work_final_staging

    task_billing_detail_work_final_staging >> task_billing_detail_merge

    