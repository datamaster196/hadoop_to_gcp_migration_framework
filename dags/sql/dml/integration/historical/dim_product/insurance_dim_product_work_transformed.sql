CREATE or REPLACE table `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_transformed` as
SELECT
COALESCE(dim_product_category.product_category_adw_key,'-1') product_category_adw_key,
COALESCE(((select Vendor_ADW_Key from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor` where vendor_cd =  'ALL' and vendor_typ = 'Insurance' and actv_ind ='Y')),'-1') vendor_adw_key,
cdpolicylinetypecode
,typeofbusinesscode ,
product_sku,
product_sku_key,
product_sku_desc,
product_sku_effective_dt,
product_sku_expiration_dt,
product_item_nbr_cd,
product_item_desc,
product_vendor_nm,
product_service_category_cd,
product_status_cd,
product_dynamic_sku,
product_dynamic_sku_desc,
product_dynamic_sku_identifier ,
product_unit_cost,
CAST('1900-01-01' AS datetime) AS last_upd_dt
,CAST('9999-12-31' AS datetime) effective_end_datetime
,'Y' as actv_ind
,CURRENT_DATETIME() as integrate_insert_datetime
,{{ dag_run.id }} as integrate_insert_batch_number
,CURRENT_DATETIME() as integrate_update_datetime
,{{ dag_run.id }} as integrate_update_batch_number
,TO_BASE64(MD5(CONCAT(COALESCE(typeofbusinesscode,''),COALESCE(cdpolicylinetypecode,'')))) as adw_row_hash
FROM `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_source` as source
LEFT JOIN
`{{var.value.INTEGRATION_PROJECT}}.adw.dim_product_category` dim_product_category on
source.typeofbusinesscode=dim_product_category.product_category_cd