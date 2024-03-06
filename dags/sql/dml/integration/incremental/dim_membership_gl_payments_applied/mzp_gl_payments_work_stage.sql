CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_mbrs_gl_payments_applied_work_stage` AS
SELECT
  COALESCE(target.mbrs_gl_pymt_applied_adw_key,
    GENERATE_UUID()) mbrs_gl_pymt_applied_adw_key,
  source.mbrs_adw_key,
  source.aca_office_adw_key,
  source.mbrs_payment_apd_summ_adw_key,
  source.gl_payment_journal_source_key AS gl_payment_journal_source_key,
  source.gl_payment_gl_desc,
  source.gl_payment_account_nbr,
  source.gl_payment_post_dt AS gl_payment_post_dt,
  source.gl_payment_process_dt AS gl_payment_process_dt,
  source.gl_payment_journal_amt AS gl_payment_journal_amt,
  source.gl_payment_journal_created_dtm AS gl_payment_journal_created_dtm,
  source.gl_payment_journ_d_c_cd AS gl_payment_journ_d_c_cd,
  SAFE_CAST(source.effective_start_datetime AS DATETIME) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_mbrs_gl_payments_applied_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied` target
ON
  (source.gl_payment_journal_source_key=target.gl_payment_journal_source_key
    AND target.actv_ind='Y')
WHERE
  target.gl_payment_journal_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.mbrs_gl_pymt_applied_adw_key,
  target.mbrs_adw_key,
  target.aca_office_adw_key,
  target.mbrs_payment_apd_summ_adw_key,
  target.gl_payment_journal_source_key,
  target.gl_payment_gl_desc,
  target.gl_payment_account_nbr,
  target.gl_payment_post_dt,
  target.gl_payment_process_dt,
  target.gl_payment_journal_amt,
  target.gl_payment_journal_created_dtm,
  target.gl_payment_journ_d_c_cd,
  target.effective_start_datetime,
  DATETIME_SUB(CAST(source.effective_start_datetime AS datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  target.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_mbrs_gl_payments_applied_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied` target
ON
  (source. gl_payment_journal_source_key =target. gl_payment_journal_source_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash