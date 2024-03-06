CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_stage` AS
SELECT
  COALESCE(target.mbrs_payment_apd_dtl_adw_key,
    GENERATE_UUID()) mbrs_payment_apd_dtl_adw_key,
  source.mbr_adw_key AS mbr_adw_key,
  CAST(source.mbrs_payment_apd_summ_adw_key AS string) AS mbrs_payment_apd_summ_adw_key,
  CAST (source.product_adw_key AS string) AS product_adw_key,
  CAST(source.payment_apd_dtl_source_key AS int64) AS payment_apd_dtl_source_key,
  source.payment_applied_dt AS payment_applied_dt,
  CAST(source.payment_method_cd AS string) AS payment_method_cd,
  source.payment_method_desc AS payment_method_desc,
  CAST(source.payment_detail_amt AS numeric) AS payment_detail_amt,
  CAST(source.payment_applied_unapplied_amt AS numeric) AS payment_applied_unapplied_amt,
  CAST(source.discount_amt AS numeric) AS discount_amt,
  CAST(source.pymt_apd_discount_counted AS string) AS pymt_apd_discount_counted,
  CAST(SUBSTR(source.discount_effective_dt, 0, 10) AS date) AS discount_effective_dt,
  CAST(source.rider_cost_effective_dtm AS datetime) AS rider_cost_effective_dtm,
  last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) AS effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash AS adw_row_hash,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} as integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail` target
ON
  (source.payment_apd_dtl_source_key=SAFE_CAST(target.payment_apd_dtl_source_key AS string)
    AND target.actv_ind='Y')
WHERE
  target.mbr_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.mbrs_payment_apd_dtl_adw_key,
  target.mbr_adw_key,
  target.mbrs_payment_apd_summ_adw_key,
  target.product_adw_key,
  target.payment_apd_dtl_source_key,
  target.payment_applied_dt,
  target.payment_method_cd,
  target.payment_method_desc,
  target.payment_detail_amt,
  target.payment_applied_unapplied_amt,
  target.discount_amt,
  source.pymt_apd_discount_counted,
  target.discount_effective_dt,
  target.rider_cost_effective_dtm,
  target.effective_start_datetime,
  DATETIME_SUB(source.last_upd_dt,
    INTERVAL 1 day) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail` target
ON
  (source.payment_apd_dtl_source_key=SAFE_CAST(target.payment_apd_dtl_source_key AS string)
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash