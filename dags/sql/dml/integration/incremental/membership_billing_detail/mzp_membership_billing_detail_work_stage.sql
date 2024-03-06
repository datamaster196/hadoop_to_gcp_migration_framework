CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_detail_work_final_stage` AS
SELECT
  COALESCE(target.mbrs_billing_detail_adw_key,
    GENERATE_UUID()) AS mbrs_billing_detail_adw_key,
  source.mbrs_billing_summary_adw_key,
  COALESCE(source.mbr_adw_key,
    '-1') AS mbr_adw_key,
  COALESCE(source.product_adw_key,
    '-1') AS product_adw_key,
  SAFE_CAST(source.bill_detail_source_key AS INT64) AS bill_detail_source_key,
  SAFE_CAST(source.bill_summary_source_key AS INT64) AS bill_summary_source_key,
  CAST(source.bill_process_dt AS Date) AS bill_process_dt,
  CAST(source.bill_notice_nbr AS INT64) AS bill_notice_nbr,
  source.bill_typ,
  CAST(source.bill_detail_amt AS NUMERIC) AS bill_detail_amt,
  source.bill_detail_text,
  source.rider_billing_category_cd,
  source.rider_billing_category_desc,
  source.rider_solicit_cd,
  source.last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_detail_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail` target
ON
  (source.bill_detail_source_key =SAFE_CAST(target.bill_detail_source_key AS STRING)
    AND target.actv_ind='Y')
WHERE
  target.mbrs_billing_detail_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.mbrs_billing_detail_adw_key,
  target.mbrs_billing_summary_adw_key,
  target.mbr_adw_key,
  target.product_adw_key,
  target.bill_detail_source_key,
  target.bill_summary_source_key,
  target.bill_process_dt,
  target.bill_notice_nbr,
  target.bill_typ,
  target.bill_detail_amt,
  target.bill_detail_text,
  target.rider_billing_category_cd,
  target.rider_billing_category_desc,
  target.rider_solicit_cd,
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_detail_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail` target
ON
  (source.bill_detail_source_key =SAFE_CAST(target.bill_detail_source_key AS STRING)
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash