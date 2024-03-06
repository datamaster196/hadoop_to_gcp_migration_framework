CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_final_stage` AS
SELECT
  COALESCE(target.mbrs_billing_summary_adw_key,
    GENERATE_UUID()) AS mbrs_billing_summary_adw_key,
  source.mbrs_adw_key AS mbrs_adw_key,
  SAFE_CAST(source.bill_summary_source_key AS INT64) AS bill_summary_source_key,
  CAST(substr (source.bill_process_dt,
      0,
      10) AS Date) AS bill_process_dt,
  CAST(source.bill_notice_nbr AS INT64) AS bill_notice_nbr,
  CAST(source.bill_amt AS NUMERIC) AS bill_amt,
  CAST(source.bill_credit_amt AS NUMERIC) AS bill_credit_amt,
  source.bill_typ,
  CAST(substr (source.mbr_expiration_dt,
      0,
      10) AS Date) AS mbr_expiration_dt,
  CAST(source.bill_paid_amt AS NUMERIC) AS bill_paid_amt,
  source.bill_renewal_method,
  source.bill_panel_cd,
  source.bill_ebilling_ind,
  source.last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary` target
ON
  (source.bill_summary_source_key =SAFE_CAST(target.bill_summary_source_key AS STRING)
    AND target.actv_ind='Y')
WHERE
  target.mbrs_billing_summary_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.mbrs_billing_summary_adw_key,
  target.mbrs_adw_key,
  target.bill_summary_source_key,
  target.bill_process_dt,
  target.bill_notice_nbr,
  target.bill_amt,
  target.bill_credit_amt,
  target.bill_typ,
  target.mbr_expiration_dt,
  target.bill_paid_amt,
  target.bill_renewal_method,
  target.bill_panel_cd,
  target.bill_ebilling_ind,
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary` target
ON
  (source.bill_summary_source_key =SAFE_CAST(target.bill_summary_source_key AS STRING)
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash