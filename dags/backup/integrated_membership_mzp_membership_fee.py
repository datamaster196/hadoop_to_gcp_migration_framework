from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_mzp_membership_fee"

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
# Load membership_fee Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_fee_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_source` AS
SELECT
membership_fee_source.MEMBERSHIP_FEES_KY,
membership_fee_source.membership_ky,
membership_fee_source.status,
membership_fee_source.fee_type,
membership_fee_source.fee_dt,
membership_fee_source.waived_dt,
membership_fee_source.waived_by,
membership_fee_source.donor_nr,
membership_fee_source.waived_reason_cd,
membership_fee_source.last_upd_dt, 
  ROW_NUMBER() OVER(PARTITION BY membership_fee_source.membership_ky  ORDER BY membership_fee_source.last_upd_dt desc) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.membership_fees` AS membership_fee_source
 WHERE
   CAST(membership_fee_source.last_upd_dt AS datetime) > (
   SELECT
    MAX(effective_start_datetime)
   FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_fee_work_transformed = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` AS
SELECT
  COALESCE(dim_member.member_adw_key,
    "-1") AS member_adw_key,
  COALESCE(product1.product_adw_key,
    "-1") AS product_adw_key,
  SAFE_CAST(COALESCE(membership_fee_source.MEMBERSHIP_FEES_KY,
    "-1")as int64) AS membership_fee_source_key,
  membership_fee_source.status AS status,
  membership_fee_source.fee_type AS fee_type,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.fee_dt,1,10)) AS Date) AS fee_date,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.waived_dt,1,10)) AS Date) AS waived_date,
  SAFE_CAST(membership_fee_source.waived_by as INT64)  AS waived_by,
  membership_fee_source.donor_nr AS donor_number,
  membership_fee_source.waived_reason_cd AS waived_reason_code,
  membership_fee_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(dim_member.member_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(product1.product_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(membership_fee_source.MEMBERSHIP_FEES_KY,
            "-1"),
          ''), '|', ifnull(membership_fee_source.status,
          ''), '|', ifnull(membership_fee_source.fee_type,
          ''), '|',ifnull(membership_fee_source.fee_dt,
          ''), '|',ifnull(membership_fee_source.waived_dt,
          ''), '|',ifnull(membership_fee_source.waived_by,
          ''), '|',ifnull(membership_fee_source.donor_nr,
          ''), '|',ifnull(membership_fee_source.waived_reason_cd,
          '') ))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_source` AS membership_fee_source
LEFT OUTER JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_member` dim_member
ON
  membership_fee_source.MEMBERSHIP_FEES_KY=CAST(dim_member.member_source_system_key AS string)
  AND dim_member.active_indicator='Y'
LEFT OUTER JOIN (
  SELECT
    membership.membership_ky,
    product_adw_key,
    product_sku_key,
    active_indicator,
    ROW_NUMBER() OVER(PARTITION BY membership.membership_ky ORDER BY NULL ) AS dupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_product` product,
    `{iu.INGESTION_PROJECT}.mzp.membership` membership
  WHERE
    membership.coverage_level_cd = product.product_sku_key
    AND product.active_indicator='Y' ) product1
ON
  membership_fee_source.membership_ky=product1.membership_ky
  AND product1.dupe_check=1
WHERE
  membership_fee_source.dupe_check=1
"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_fee_work_final_staging = f"""


CREATE OR REPLACE TABLE  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_stage` AS
SELECT 
   COALESCE(target.membership_fee_adw_key,
      GENERATE_UUID()) membership_fee_adw_key,
  source.member_adw_key,
  source.product_adw_key,
  source.membership_fee_source_key  as membership_fee_source_key,
  source.status,
  source.fee_type,
  source.fee_date,
  source.waived_date,
  source.waived_by as waived_by,
  source.donor_number as donor_number,
  source.waived_reason_code as waived_reason_code ,
  SAFE_CAST(source.effective_start_datetime as DATETIME) as effective_start_datetime ,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  1 integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  1 integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` target
ON
  (source.membership_fee_source_key=target.membership_fee_source_key
    AND target.active_indicator='Y')
WHERE
  target.membership_fee_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  membership_fee_adw_key,
  target.member_adw_key,
  target.product_adw_key,
  target.membership_fee_source_key,
  target.status,
  target.fee_type,
  target.fee_date,
  target.waived_date,
  target.waived_by,
  target.donor_number,
  target.waived_reason_code ,
  target.effective_start_datetime,
  DATETIME_SUB(cast(source.effective_start_datetime as datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1 
 FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` target
ON
  (source.membership_fee_source_key=target.membership_fee_source_key 
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash
    
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership_fee
membership_fee_merge = f"""

MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_stage` b
ON
  (a.membership_fee_source_key = b.membership_fee_source_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  membership_fee_adw_key, 
  member_adw_key , 
  product_adw_key , 
  membership_fee_source_key , 
  status , 
  fee_type , 
  fee_date , 
  waived_date,
  waived_by,
  donor_number,
  waived_reason_code,
  effective_start_datetime, 
  effective_end_datetime, 
  active_indicator, 
  adw_row_hash, 
  integrate_insert_datetime, 
  integrate_insert_batch_number, 
  integrate_update_datetime, 
  integrate_update_batch_number ) VALUES (
  b.membership_fee_adw_key , 
  b.member_adw_key , 
  b.product_adw_key , 
  b.membership_fee_source_key , 
  b.status , 
  b.fee_type , 
  b.fee_date , 
  b.waived_date,
  b.waived_by,
  b.donor_number,
  b.waived_reason_code,
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

    # Membership_fee

    # Membership_fee - Workarea loads
    task_membership_fee_work_source = int_u.run_query("build_membership_fee_work_source", membership_fee_work_source)
    task_membership_fee_work_transformed = int_u.run_query("build_membership_fee_work_transformed", membership_fee_work_transformed)
    task_membership_fee_work_final_staging = int_u.run_query("build_membership_fee_work_final_staging",
                                                         membership_fee_work_final_staging)

    # Membership_fee - Merge Load
    task_membership_fee_merge = int_u.run_query('merge_membership_fee', membership_fee_merge)


    ######################################################################
    # DEPENDENCIES



    # Membership_fee

    task_membership_fee_work_source >> task_membership_fee_work_transformed
    task_membership_fee_work_transformed >> task_membership_fee_work_final_staging

    task_membership_fee_work_final_staging >> task_membership_fee_merge
    

