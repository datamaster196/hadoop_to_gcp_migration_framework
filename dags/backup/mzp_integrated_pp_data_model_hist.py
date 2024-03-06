from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_mzp_paymentplan_hist"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22),
    'email': ['airflow@example.com'],
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

######################################################################
# Load Payment Plan Historical set of Data
######################################################################

paymentplan_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_source`  as 
SELECT 
 PLAN_BILLING_KY,
 RIDER_KY,
 source.PAYMENT_PLAN_KY,
 PAYMENT_NUMBER,
 MEMBERSHIP_PAYMENT_KY,
 PAYMENT_STATUS,
 CHARGE_DT,
 CHARGE_CT,
 PAYMENT_AT,
 source.LAST_UPD_DT,
 COST_EFFECTIVE_DT,
 MEMBERSHIP_FEES_KY,
 DONATION_HISTORY_KY,
 MEMBERSHIP_KY,
 pp.PLAN_NAME,
 pp.NUMBER_OF_PAYMENTS,
 pp.PLAN_LENGTH,
 ROW_NUMBER() OVER(PARTITION BY source.plan_billing_ky ORDER BY source.last_upd_dt DESC) AS dupe_check
FROM
`{iu.INGESTION_PROJECT}.mzp.plan_billing` as source
left join `{iu.INGESTION_PROJECT}.mzp.payment_plan` as pp on source.PAYMENT_PLAN_KY = pp.PAYMENT_PLAN_KY
"""

paymentplan_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` as 
SELECT 
 coalesce(adw_memship.membership_adw_key,'-1') as membership_adw_key,
 coalesce(adw_rider.member_rider_adw_key,'-1') as member_rider_adw_key,
 cast(PLAN_BILLING_KY as int64) as payment_plan_source_key ,
 source.PLAN_NAME as payment_plan_name,
 cast(source.NUMBER_OF_PAYMENTS as int64) as payment_plan_payment_count,
 cast(source.PLAN_LENGTH as int64) as payment_plan_months,
 cast(source.PAYMENT_NUMBER as int64) as paymentnumber,
 source.PAYMENT_STATUS as payment_plan_status,
 cast(cast(source.CHARGE_DT as datetime) as date) as payment_plan_charge_date,
 case when source.DONATION_HISTORY_KY is null then 'N' else 'Y' end as safety_fund_donation_indicator,
 CAST(source.last_upd_dt AS datetime) AS last_upd_dt,
 CAST('9999-12-31' AS datetime) effective_end_datetime,
 'Y' as active_indicator,
 TO_BASE64(MD5(CONCAT(
            ifnull(source.PLAN_BILLING_KY,''),'|',
            ifnull(source.membership_ky,''),'|',
            ifnull(source.rider_ky,''),'|',
            ifnull(source.PLAN_NAME,''),'|',
            ifnull(source.NUMBER_OF_PAYMENTS,''),'|',
            ifnull(source.PLAN_LENGTH,''),'|',
            ifnull(source.PAYMENT_NUMBER,''),'|',
            ifnull(source.PAYMENT_STATUS,''),'|',
            ifnull(source.CHARGE_DT,''),'|',
            ifnull(source.DONATION_HISTORY_KY,'')
            ))) as adw_row_hash,
 CURRENT_DATETIME() as integrate_insert_datetime,
 1 as integrate_insert_batch_number,
 CURRENT_DATETIME() as integrate_update_datetime,
 1 as integrate_update_batch_number
FROM
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_source` as source 
 LEFT JOIN `{int_u.INTEGRATION_PROJECT}.member.dim_membership` adw_memship
 on source.membership_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING) AND adw_memship.active_indicator='Y'
 LEFT JOIN `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider` adw_rider
 on source.rider_ky =SAFE_CAST(adw_rider.member_rider_source_key AS STRING) AND adw_rider.active_indicator='Y'
WHERE source.dupe_check=1 
"""

paymentplan_type2_logic = f"""

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_type_2_hist` AS
WITH
  paymentplan_hist AS (
  SELECT
    payment_plan_source_key,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY payment_plan_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY payment_plan_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` 
  ), set_grouping_column AS (
  SELECT
    payment_plan_source_key,
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
    paymentplan_hist 
  ), set_groups AS (
  SELECT
    payment_plan_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY payment_plan_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    payment_plan_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    payment_plan_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  payment_plan_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""
paymentplan_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` 
  ( membership_payment_plan_adw_key ,    
  membership_adw_key,
  member_rider_adw_key,
  payment_plan_source_key,    
  payment_plan_name,
  payment_plan_payment_count,
  payment_plan_months,
  paymentnumber,
  payment_plan_status,
  payment_plan_charge_date,
  safety_fund_donation_indicator,
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
  membership_payment_plan_adw_key ,    
  source.membership_adw_key,
  source.member_rider_adw_key,
  source.payment_plan_source_key,   
  source.payment_plan_name, 
  source.payment_plan_payment_count,
  source.payment_plan_months,
  source.paymentnumber,
  source.payment_plan_status,
  source.payment_plan_charge_date,
  source.safety_fund_donation_indicator,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` source ON (
  source.payment_plan_source_key=hist_type_2.payment_plan_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_payment_plan_adw_key,
    payment_plan_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed`
  GROUP BY
    payment_plan_source_key) pk
ON (source.payment_plan_source_key=pk.payment_plan_source_key)

"""

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    # Payment Plan
    task_payment_plan_source = int_u.run_query('paymentplan_source', paymentplan_source)
    task_payment_plan_transformed = int_u.run_query('paymentplan_transformed', paymentplan_transformed)
    task_payment_plan_type2_logic = int_u.run_query('paymentplan_type2_logic', paymentplan_type2_logic)
    task_payment_plan_insert = int_u.run_query('paymentplan_insert', paymentplan_insert)

    ######################################################################
    # DEPENDENCIES

    # Payment Plan
    task_payment_plan_source >> task_payment_plan_transformed
    task_payment_plan_transformed >> task_payment_plan_type2_logic
    task_payment_plan_type2_logic >> task_payment_plan_insert





