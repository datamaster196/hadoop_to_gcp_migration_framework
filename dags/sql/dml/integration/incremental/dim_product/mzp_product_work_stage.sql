CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_source_stage` AS
SELECT
  GENERATE_UUID() as product_adw_key, 
  source.product_category_adw_key,
  source.product_sku,
  source.product_sku_key,
  source.vendor_adw_key,
  source.product_sku_desc,
  source.adw_row_hash,
  source.effective_start_datetime,
  source.effective_end_datetime,
  source.actv_ind,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku
    AND target.actv_ind='Y')
WHERE
  target.product_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
  target.product_adw_key,
  target.product_category_adw_key,
  target.product_sku,
  target.product_sku_key,
  target.vendor_adw_key,
  target.product_sku_desc,
  target.adw_row_hash,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 second) AS effective_end_datetime,
  'N' AS actv_ind,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  target.integrate_update_batch_number,
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash