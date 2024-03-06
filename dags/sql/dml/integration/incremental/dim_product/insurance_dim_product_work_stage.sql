CREATE or REPLACE table `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_stage` as
SELECT
  GENERATE_UUID() as product_adw_key,
  source.product_category_adw_key,
  source.product_sku,
  source.product_sku_key,
  source.vendor_adw_key,
  source.product_sku_desc,
  SAFE_CAST(source.product_sku_effective_dt as DATE) product_sku_effective_dt,
SAFE_CAST(source.product_sku_expiration_dt as DATE) product_sku_expiration_dt,
source.product_item_nbr_cd,
source.product_item_desc,
source.product_vendor_nm,
source.product_service_category_cd,
source.product_status_cd,
source.product_dynamic_sku,
source.product_dynamic_sku_desc,
-- source.product_dynamic_sku_identifier ,
SAFE_CAST(source.product_unit_cost as NUMERIC) product_unit_cost,
  source.adw_row_hash,
  source.effective_start_datetime,
  source.effective_end_datetime,
  source.actv_ind,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_transformed` source
LEFT JOIN
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku and source.product_category_adw_key=target.product_category_adw_key
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
  SAFE_CAST(target.product_sku_expiration_dt as DATE),
target.product_sku_expiration_dt,
target.product_item_nbr_cd,
target.product_item_desc,
target.product_vendor_nm,
target.product_service_category_cd,
target.product_status_cd,
target.product_dynamic_sku,
target.product_dynamic_sku_desc,
-- target.product_dynamic_sku_identifier ,
target.product_unit_cost,
  target.adw_row_hash,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 second) AS effective_end_datetime,
  'N' AS actv_ind,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_transformed` source
JOIN
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku and source.product_category_adw_key=target.product_category_adw_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash