CREATE OR REPLACE TABLE  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_stage` AS
SELECT 
   COALESCE(target.mbrs_fee_adw_key,
      GENERATE_UUID()) mbrs_fee_adw_key,
  source.mbr_adw_key,
  source.product_adw_key,
  source.mbrs_fee_source_key  as mbrs_fee_source_key,
  source.status,
  source.fee_typ,
  source.fee_dt,
  source.waived_dt,
  source.waived_by as waived_by,
  source.donor_nbr as donor_nbr,
  source.waived_reason_cd as waived_reason_cd ,
  SAFE_CAST(source.effective_start_datetime as DATETIME) as effective_start_datetime ,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee` target
ON
  (source.mbrs_fee_source_key=target.mbrs_fee_source_key
    AND target.actv_ind='Y')
WHERE
  target.mbrs_fee_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  mbrs_fee_adw_key,
  target.mbr_adw_key,
  target.product_adw_key,
  target.mbrs_fee_source_key,
  target.status,
  target.fee_typ,
  target.fee_dt,
  target.waived_dt,
  target.waived_by,
  target.donor_nbr,
  target.waived_reason_cd ,
  target.effective_start_datetime,
  DATETIME_SUB(cast(source.effective_start_datetime as datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  target.integrate_update_batch_number
 FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee` target
ON
  (source.mbrs_fee_source_key=target.mbrs_fee_source_key 
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash