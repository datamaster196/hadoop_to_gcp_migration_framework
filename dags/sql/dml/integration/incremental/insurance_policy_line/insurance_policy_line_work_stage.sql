CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_line_work_stage` AS
SELECT
  COALESCE(target.ins_line_adw_key,
    GENERATE_UUID()) ins_line_adw_key,
    source.ofc_aca_adw_key,
  source.premium_paid_vendor_key AS premium_paid_vendor_key,
  source.legal_issuing_vendor_key AS legal_issuing_vendor_key,
  source.product_adw_key AS product_adw_key,
  source.ins_policy_adw_key AS ins_policy_adw_key,
  SAFE_CAST(source.ins_policy_line_source_key AS INT64) as ins_policy_line_source_key,
  SAFE_CAST(source.ins_line_comm_percent AS NUMERIC) as ins_line_comm_percent,
  source.ins_line_delivery_method_cd as ins_line_delivery_method_cd,
  source.ins_line_delivery_method_desc as ins_line_delivery_method_desc,
  SAFE_CAST(source.ins_line_effective_dt AS DATETIME) as ins_line_effective_dt,
  SAFE_CAST(source.ins_line_Expiration_dt AS DATETIME) as ins_line_Expiration_dt,
  SAFE_CAST(source.ins_line_first_written_dt AS DATETIME) as ins_line_first_written_dt,
  source.ins_line_status as ins_line_status,
  SAFE_CAST(source.effective_start_datetime AS DATETIME) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_line_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line` target
ON
  (source.ins_policy_line_source_key=SAFE_CAST(target.ins_policy_line_source_key AS STRING)
  and source.ins_policy_adw_key=SAFE_CAST(target.ins_policy_adw_key AS STRING)
  AND target.actv_ind='Y')
WHERE
  target.ins_policy_line_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  ins_line_adw_key,
  target.ofc_aca_adw_key,
  target.premium_paid_vendor_key,
  target.legal_issuing_vendor_key,
  target.product_adw_key,
  target.ins_policy_adw_key,
  target.ins_policy_line_source_key,
  target.ins_line_comm_percent,
  target.ins_line_delivery_method_cd,
  target.ins_line_delivery_method_desc,
  target.ins_line_effective_dt,
  target.ins_line_Expiration_dt,
  target.ins_line_first_written_dt,
  target.ins_line_status,
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_line_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line` target
ON
  (source.ins_policy_line_source_key=SAFE_CAST(target.ins_policy_line_source_key AS STRING)
  and source.ins_policy_adw_key=SAFE_CAST(target.ins_policy_adw_key AS STRING)
  AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash