CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_final_stage` AS
SELECT
  COALESCE(target. mbr_ar_card_adw_key,
    GENERATE_UUID()) mbr_ar_card_adw_key,
  SAFE_CAST(source.nm_adw_key AS STRING) nm_adw_key,
  SAFE_CAST(source.address_adw_key AS STRING) address_adw_key,
  SAFE_CAST(source.mbr_source_arc_key AS INT64) mbr_source_arc_key,
  SAFE_CAST(source.mbr_arc_status_dtm AS DATETIME) mbr_arc_status_dtm,
  source.mbr_cc_typ_cd,
  SAFE_CAST(TIMESTAMP(source.mbr_cc_expiration_dt) AS DATE) mbr_cc_expiration_dt,
  source.mbr_cc_reject_reason_cd,
  SAFE_CAST(TIMESTAMP(source.mbr_cc_reject_dt) AS DATE) mbr_cc_reject_dt,
  source.mbr_cc_donor_nbr,
  source.mbr_cc_last_four,
  source.mbr_credit_debit_typ_cd,
  last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` target
ON
  (SAFE_CAST(source.mbr_source_arc_key AS INT64)=target.mbr_source_arc_key
    AND target.actv_ind='Y')
WHERE
  target.mbr_source_arc_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.mbr_ar_card_adw_key,
  target.nm_adw_key,
  target.address_adw_key,
  target.mbr_source_arc_key,
  target.mbr_arc_status_dtm,
  target.mbr_cc_typ_cd,
  target.mbr_cc_expiration_dt,
  target.mbr_cc_reject_reason_cd,
  target.mbr_cc_reject_dt,
  target.mbr_cc_donor_nbr,
  target.mbr_cc_last_four,
  target.mbr_credit_debit_typ_cd,
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` target
ON
  (SAFE_CAST(source.mbr_source_arc_key AS INT64)=target.mbr_source_arc_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash