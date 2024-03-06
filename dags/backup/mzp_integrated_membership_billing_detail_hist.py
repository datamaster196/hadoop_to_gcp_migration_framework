from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_billing_detail_hist"

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
# Billing_detail_hist
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
  SELECT
 billing_detail_source.bill_detail_ky as bill_detail_source_key,
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
  billing_detail_source.last_upd_dt IS NOT NULL
  and billing_detail_source.last_upd_dt < '2019-05-27 17:05:20.0'

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
 where dupe_check = 1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_detail_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_type_2_hist` AS
WITH
  membership_billing_detail_hist AS (
  SELECT
    bill_detail_source_key ,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY bill_detail_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY bill_detail_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    bill_detail_source_key,
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
    membership_billing_detail_hist 
  ), set_groups AS (
  SELECT
    bill_detail_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY bill_detail_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    bill_detail_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    bill_detail_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  bill_detail_source_key,
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

# membership_billing_detail
membership_billing_detail_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail`
  (
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
  ) 
SELECT
membership_billing_detail_adw_key,
source.membership_billing_summary_adw_key,
COALESCE(source.member_adw_key,'-1') ,
COALESCE(source.product_adw_key,'-1') ,
SAFE_CAST(source.bill_detail_source_key AS INT64),
SAFE_CAST(source.bill_summary_source_key AS INT64),
source.bill_process_date,
CAST(source.bill_notice_number AS INT64),
source.bill_type,
CAST(source.bill_detail_amount AS NUMERIC),
source.bill_detail_text,
source.rider_billing_category_code,
source.rider_billing_category_description,
source.rider_solicitation_code,
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
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` source
  ON (
  source.bill_detail_source_key=hist_type_2.bill_detail_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_billing_detail_adw_key,
    bill_detail_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed`
  GROUP BY
    bill_detail_source_key) pk
ON (source.bill_detail_source_key=pk.bill_detail_source_key)
  

"""


with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS
    
    # membership_billing_detail
    
    # membership_billing_detail - Workarea loads
    task_membership_billing_detail_work_source = int_u.run_query("build_membership_billing_detail_work_source", membership_billing_detail_work_source)
    task_membership_billing_detail_work_transformed = int_u.run_query("build_membership_billing_detail_work_transformed", membership_billing_detail_work_transformed)
    task_membership_billing_detail_work_type2_logic = int_u.run_query("build_membership_billing_detail_work_type2_logic", membership_billing_detail_work_type2_logic)
    
    # membership_billing_detail - Merge Load
    task_membership_billing_detail_insert = int_u.run_query('merge_membership_billing_detail', membership_billing_detail_insert)
    
    
    # membership_billing_detail
     

    task_membership_billing_detail_work_source >> task_membership_billing_detail_work_transformed
    task_membership_billing_detail_work_transformed >> task_membership_billing_detail_work_type2_logic
    
    task_membership_billing_detail_work_type2_logic >> task_membership_billing_detail_insert