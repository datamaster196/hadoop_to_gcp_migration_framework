CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_transformed` AS
SELECT
  coalesce(adw_memship.mbrs_adw_key,
    '-1') AS mbrs_adw_key,
  coalesce(adw_rider.mbr_rider_adw_key,
    '-1') AS mbr_rider_adw_key,
  CAST(PLAN_BILLING_KY AS int64) AS payment_plan_source_key,
  source.PLAN_NAME AS payment_plan_nm,
  CAST(source.NUMBER_OF_PAYMENTS AS int64) AS payment_plan_payment_cnt,
  CAST(source.PLAN_LENGTH AS int64) AS payment_plan_month,
  CAST(source.PAYMENT_NUMBER AS int64) AS payment_nbr,
  source.PAYMENT_STATUS AS payment_plan_status,
  CAST(CAST(source.CHARGE_DT AS datetime) AS date) AS payment_plan_charge_dt,
  CASE
    WHEN source.DONATION_HISTORY_KY IS NULL THEN 'N'
  ELSE
  'Y'
END
  AS safety_fund_donation_ind,
  CAST(source.PAYMENT_AT AS numeric) AS payment_amt,
  CAST(source.last_upd_dt AS datetime) AS last_upd_dt,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  TO_BASE64(MD5(CONCAT( ifnull(source.PLAN_BILLING_KY,
          ''),'|', ifnull(source.membership_ky,
          ''),'|', ifnull(source.rider_ky,
          ''),'|', ifnull(source.PLAN_NAME,
          ''),'|', ifnull(source.NUMBER_OF_PAYMENTS,
          ''),'|', ifnull(source.PLAN_LENGTH,
          ''),'|', ifnull(source.PAYMENT_NUMBER,
          ''),'|', ifnull(source.PAYMENT_STATUS,
          ''),'|', ifnull(source.CHARGE_DT,
          ''),'|', ifnull(source.DONATION_HISTORY_KY,
          ''),'|', ifnull(source.PAYMENT_AT,
          '') ))) AS adw_row_hash,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} AS integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} AS integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_source` AS source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` adw_memship
ON
  source.membership_ky =SAFE_CAST(adw_memship.mbrs_source_system_key AS STRING)
  AND adw_memship.actv_ind='Y'
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider` adw_rider
ON
  source.rider_ky =SAFE_CAST(adw_rider.mbr_rider_source_key AS STRING)
  AND adw_rider.actv_ind='Y'
WHERE
  source.dupe_check=1