from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_mzp_Membership_solicitation_hist"

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
# Load Membership_solicitation Historical set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

Membership_solicitation_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
 CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_solicitation_work_source` AS
  SELECT
  solicitation_source.solicitation_ky,
  solicitation_source.solicitation_cd,
  solicitation_source.group_cd,
  solicitation_source.membership_type_cd,
  solicitation_source.campaign_cd,
  solicitation_source.solicitation_category_cd,
  solicitation_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY solicitation_source.solicitation_ky ORDER BY solicitation_source.last_upd_dt DESC) AS dupe_check
  FROM `{iu.INGESTION_PROJECT}.mzp.solicitation` solicitation_source
  
"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

Membership_solicitation_work_transformed = f"""
 
  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` AS
SELECT
solicitation.solicitation_ky,
  solicitation.solicitation_cd,
  solicitation.group_cd,
  solicitation.membership_type_cd,
  solicitation.campaign_cd,
  solicitation.solicitation_category_cd,
  discount.discount_cd,
  solicitation.last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(solicitation.solicitation_cd,''),'|',
ifnull(solicitation.group_cd,''),'|',
ifnull(solicitation.membership_type_cd,''),'|',
ifnull(solicitation.campaign_cd,''),'|',
ifnull(solicitation.solicitation_category_cd,''),'|',
ifnull(discount.discount_cd,''),'|'
))) as adw_row_hash
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_solicitation_work_source` solicitation
LEFT JOIN
(SELECT 
      solicitation_ky,
      discount_cd,
      ROW_NUMBER() OVER(PARTITION BY solicitation_ky ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
    FROM `{iu.INGESTION_PROJECT}.mzp.solicitation_discount`
) discount
on discount.solicitation_ky=solicitation.solicitation_ky and discount.dupe_check=1
where solicitation.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

Membership_solicitation_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_type_2_hist` AS
WITH
  solicitation_hist AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY solicitation_ky ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(SAFE_CAST(last_upd_dt AS DATETIME)) OVER (PARTITION BY solicitation_ky ORDER BY last_upd_dt),datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    solicitation_ky,
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
    solicitation_hist 
  ), set_groups AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY solicitation_ky ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    solicitation_ky,
    adw_row_hash,
    grouping_column 
  )
SELECT
  solicitation_ky,
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

# Membership_solicitation
Membership_solicitation_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation`
(
membership_solicitation_adw_key,
membership_solicitation_source_system_key,
solicitation_code,
solicitation_group_code,
member_type_code,
solicitation_campaign_code,
solicitation_category_code,
solicitation_discount_code,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
(
SELECT
membership_solicitation_adw_key,
SAFE_CAST(source.solicitation_ky AS INT64),
source.solicitation_cd,
  source.group_cd,
  source.membership_type_cd,
  source.campaign_cd,
  source.solicitation_category_cd,
  source.discount_cd,
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
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` source
  ON (
  source.solicitation_ky=hist_type_2.solicitation_ky
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_solicitation_adw_key,
    solicitation_ky
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed`
  GROUP BY
    solicitation_ky) pk
ON (source.solicitation_ky=pk.solicitation_ky)
  )

"""



with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS


    # Membership_solicitation

    # Membership_solicitation - Workarea loads
    task_Membership_solicitation_work_source = int_u.run_query("build_Membership_solicitation_work_source", Membership_solicitation_work_source)
    task_Membership_solicitation_work_transformed = int_u.run_query("build_Membership_solicitation_work_transformed", Membership_solicitation_work_transformed)
    task_Membership_solicitation_work_type2_logic = int_u.run_query("build_Membership_solicitation_work_type2_logic",
                                                         Membership_solicitation_work_type2_logic)

    # Membership_solicitation - Merge Load
    task_Membership_solicitation_insert = int_u.run_query('merge_Membership_solicitation', Membership_solicitation_insert)



    ######################################################################
    # DEPENDENCIES

    
    # Membership_solicitation

    task_Membership_solicitation_work_source >> task_Membership_solicitation_work_transformed
    task_Membership_solicitation_work_transformed >> task_Membership_solicitation_work_type2_logic

    task_Membership_solicitation_work_type2_logic >> task_Membership_solicitation_insert
    

    
    