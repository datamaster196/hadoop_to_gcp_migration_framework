CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_work_stage` AS
SELECT
  COALESCE(target.ins_policy_adw_key,GENERATE_UUID()) AS ins_policy_adw_key,
  source.channel_adw_key,
  source.biz_line_adw_key,
  source.product_category_adw_key,
  source.ins_quote_adw_key,
  source.state_adw_key,
  source.emp_adw_key,
  CAST(source.ins_policy_system_source_key AS INT64) ins_policy_system_source_key,
  source.ins_policy_quote_ind,
  source.ins_policy_number,
  SAFE_CAST(source.ins_policy_effective_dt AS DATETIME) ins_policy_effective_dt,
  SAFE_CAST(source.ins_policy_Expiration_dt AS DATETIME) ins_policy_Expiration_dt,
  SAFE_CAST(source.ins_policy_cntrctd_exp_dt AS DATETIME) ins_policy_cntrctd_exp_dt,
  SAFE_CAST(source.ins_policy_annualized_comm AS NUMERIC) ins_policy_annualized_comm,
  SAFE_CAST(source.ins_policy_annualized_premium AS NUMERIC) ins_policy_annualized_premium,
  SAFE_CAST(source.ins_policy_billed_comm AS NUMERIC) ins_policy_billed_comm,
  SAFE_CAST(source.ins_policy_billed_premium AS NUMERIC) ins_policy_billed_premium,
  SAFE_CAST(source.ins_policy_estimated_comm AS NUMERIC) ins_policy_estimated_comm,
  SAFE_CAST(source.ins_policy_estimated_premium AS NUMERIC) ins_policy_estimated_premium,
  source.effective_start_datetime,
  source.effective_end_datetime,
  source.actv_ind,
  source.adw_row_hash,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` target
ON
  ( SAFE_CAST(source.ins_policy_system_source_key AS INT64)=target.ins_policy_system_source_key
  and source.ins_policy_quote_ind=target.ins_policy_quote_ind
    AND target.actv_ind='Y')
WHERE
  target.ins_policy_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.ins_policy_adw_key,
  target.channel_adw_key,
  target.biz_line_adw_key,
  target.product_category_adw_key,
  target.ins_quote_adw_key,
  target.state_adw_key,
  target.emp_adw_key,
  target.ins_policy_system_source_key,
  target.ins_policy_quote_ind,
  target.ins_policy_number,
  target.ins_policy_effective_dt,
  target.ins_policy_Expiration_dt,
  target.ins_policy_cntrctd_exp_dt,
  target.ins_policy_annualized_comm,
  target.ins_policy_annualized_premium,
  target.ins_policy_billed_comm,
  target.ins_policy_billed_premium,
  target.ins_policy_estimated_comm,
  target.ins_policy_estimated_premium,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 second) AS effective_end_datetime,
  "N" AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  target.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` target
ON
  (SAFE_CAST(source.ins_policy_system_source_key AS INT64)=target.ins_policy_system_source_key
  and source.ins_policy_quote_ind=target.ins_policy_quote_ind
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash