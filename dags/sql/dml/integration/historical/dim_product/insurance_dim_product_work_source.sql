CREATE or REPLACE table `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_source`  as
SELECT
cdpolicylinetypecode,
typeofbusinesscode ,
cdpolicylinetypecode as product_sku,
cdpolicylinetypecode as product_sku_key,
cdpolicylinetypecode as product_sku_desc,
'' as product_sku_effective_dt,
'' as product_sku_expiration_dt,
'' as product_item_nbr_cd,
'' as product_item_desc,
'' as product_vendor_nm,
'' as product_service_category_cd,
'' as product_status_cd,
'' as product_dynamic_sku,
'' as product_dynamic_sku_desc,
'' as product_dynamic_sku_identifier ,
'' as product_unit_cost,
ROW_NUMBER() OVER(PARTITION BY source.cdpolicylinetypecode,source.typeofbusinesscode ORDER BY  NULL DESC) AS dupe_check
FROM
`{{ var.value.INGESTION_PROJECT }}.epic.cdpolicylinetype` as source