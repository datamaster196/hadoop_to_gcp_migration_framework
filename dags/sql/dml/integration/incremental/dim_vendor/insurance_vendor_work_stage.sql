 CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_stage` AS
SELECT
    COALESCE(target.vendor_adw_key,
      GENERATE_UUID()) vendor_adw_key,
      source.biz_line_adw_key,
source.address_adw_key,
source.email_adw_key,
source.vendor_cd,
source.vendor_name,
source.vendor_typ,
source.vendor_contact,
source.preferred_vendor,
source.vendor_status,
source.effective_start_datetime AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS actv_ind,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    {{ dag_run.id }} integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    {{ dag_run.id }} integrate_update_batch_number
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_transformed` source
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor` target
  ON
    (source.vendor_cd =target.vendor_cd
      AND target.actv_ind='Y')
  WHERE
    target.vendor_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
    UNION ALL
    SELECT
     target.vendor_adw_key,
target.biz_line_adw_key,
target.address_adw_key,
target.email_adw_key,
target.vendor_cd,
target.vendor_name,
target.vendor_typ,
target.vendor_contact,
target.preferred_vendor,
target.vendor_status,
     target.effective_start_datetime,
    DATETIME_SUB(source.effective_start_datetime ,
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS actv_ind,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    target.integrate_update_batch_number
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_transformed` source
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor` target
  ON
    (source.vendor_cd =target.vendor_cd
      AND target.actv_ind='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash
