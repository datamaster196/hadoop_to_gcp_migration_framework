CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_stage` AS
SELECT
  GENERATE_UUID() AS mbrs_payment_plan_adw_key,
  source.mbrs_adw_key,
  source.mbr_rider_adw_key,
  source.payment_plan_source_key,
  source.payment_plan_nm,
  source.payment_plan_payment_cnt,
  source.payment_plan_month,
  source.payment_nbr,
  source.payment_plan_status,
  source.payment_plan_charge_dt,
  source.safety_fund_donation_ind ,
  source.payment_amt,
  last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  source.actv_ind,
  source.adw_row_hash,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan` target
ON
  (source.payment_plan_source_key=target.payment_plan_source_key
    AND target.actv_ind='Y')
WHERE
  target.payment_plan_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  DISTINCT target.mbrs_payment_plan_adw_key,
  target.mbrs_adw_key,
  target.mbr_rider_adw_key,
  target.payment_plan_source_key,
  target.payment_plan_nm,
  target.payment_plan_payment_cnt,
  target.payment_plan_month,
  target.payment_nbr,
  target.payment_plan_status,
  target.payment_plan_charge_dt,
  target.safety_fund_donation_ind ,
  target.payment_amt,
  target.effective_start_datetime,
  DATETIME_SUB(source.last_upd_dt,
    INTERVAL 1 second) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  target.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan` target
ON
  (source.payment_plan_source_key=target.payment_plan_source_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash