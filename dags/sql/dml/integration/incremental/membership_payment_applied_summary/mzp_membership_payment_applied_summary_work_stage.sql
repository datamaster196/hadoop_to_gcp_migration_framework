CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_source_stage` AS
SELECT
  GENERATE_UUID() AS mbrs_payment_apd_summ_adw_key,
  source.mbrs_payment_apd_rev_adw_key,
  source.payment_applied_source_key,
  source.mbrs_adw_key,
  source.mbrs_payment_received_adw_key,
  source.aca_office_adw_key,
  source.emp_adw_key,
  source.payment_applied_dt,
  source.payment_applied_amt,
  source.payment_batch_nm,
  source.payment_method_cd,
  source.payment_method_desc,
  source.payment_typ_cd,
  source.payment_typ_desc,
  source.payment_adj_cd,
  source.payment_adj_desc,
  source.payment_created_dtm,
  source.payment_paid_by_cd,
  source.payment_unapplied_amt,
  source.advance_payment_amt,
  source.payment_source_cd,
  source.payment_source_desc,
  source.payment_tran_typ_cd,
  source.payment_tran_typ_cdesc,
  source.effective_start_datetime,
  source.effective_end_datetime,
  source.actv_ind,
  source.adw_row_hash,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary` target
ON
  (source.payment_applied_source_key=target.payment_applied_source_key
    AND target.actv_ind='Y')
WHERE
  target.mbrs_payment_apd_summ_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  DISTINCT mbrs_payment_apd_summ_adw_key,
  target.mbrs_payment_apd_rev_adw_key,
  target.payment_applied_source_key,
  target.mbrs_adw_key,
  target.mbrs_payment_received_adw_key,
  target.aca_office_adw_key,
  target.emp_adw_key,
  target.payment_applied_dt,
  target.payment_applied_amt,
  target.payment_batch_nm,
  target.payment_method_cd,
  target.payment_method_desc,
  target.payment_typ_cd,
  target.payment_typ_desc,
  target.payment_adj_cd,
  target.payment_adj_desc,
  target.payment_created_dtm,
  target.payment_paid_by_cd,
  target.payment_unapplied_amt,
  target.advance_payment_amt,
  target.payment_source_cd,
  target.payment_source_desc,
  target.payment_tran_typ_cd,
  target.payment_tran_typ_cdesc,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 second) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  target.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary` target
ON
  (source.payment_applied_source_key=target.payment_applied_source_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash