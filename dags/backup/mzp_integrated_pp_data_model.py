from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_mzp_paymentplan"

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
# Load Payment Plan Incremental set of Data
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
where CAST(source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan`)
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

paymentplan_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_stage` AS
SELECT
  GENERATE_UUID() as membership_payment_plan_adw_key, 
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
  last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  source.active_indicator,
  source.adw_row_hash,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` target
ON
  (source.payment_plan_source_key=target.payment_plan_source_key AND target.active_indicator='Y')
WHERE
  target.payment_plan_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
  target.membership_payment_plan_adw_key,
  target.membership_adw_key,
  target.member_rider_adw_key,
  target.payment_plan_source_key,
  target.payment_plan_name,
  target.payment_plan_payment_count,
  target.payment_plan_months,
  target.paymentnumber,
  target.payment_plan_status,
  target.payment_plan_charge_date,
  target.safety_fund_donation_indicator,
  target.effective_start_datetime,
  DATETIME_SUB(source.last_upd_dt, INTERVAL 1 day) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` target
ON
  (source.payment_plan_source_key=target.payment_plan_source_key
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""

paymentplan_merge = f"""
MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_stage` b
ON
  (a.membership_payment_plan_adw_key = b.membership_payment_plan_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  membership_payment_plan_adw_key ,    
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
  effective_end_datetime,
  active_indicator,
  adw_row_hash,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number
    
  ) VALUES (
  b.membership_payment_plan_adw_key ,    
  b.membership_adw_key,
  b.member_rider_adw_key,
  b.payment_plan_source_key,
  b.payment_plan_name,    
  b.payment_plan_payment_count,
  b.payment_plan_months,
  b.paymentnumber,
  b.payment_plan_status,
  b.payment_plan_charge_date,
  b.safety_fund_donation_indicator,
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

    # Product
    task_paymentplan_source = int_u.run_query('paymentplan_source', paymentplan_source)
    task_paymentplan_transformed = int_u.run_query('paymentplan_transformed', paymentplan_transformed)
    task_paymentplan_stage = int_u.run_query('paymentplan_stage', paymentplan_stage)
    task_paymentplan_merge = int_u.run_query('paymentplan_merge', paymentplan_merge)

    ######################################################################
    # DEPENDENCIES

    # Product
    task_paymentplan_source >> task_paymentplan_transformed
    task_paymentplan_transformed >> task_paymentplan_stage
    task_paymentplan_stage >> task_paymentplan_merge

    #task_paymentplan_merge >> task_membership_work_source

