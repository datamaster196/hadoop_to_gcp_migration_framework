CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_transformed` as 
SELECT 
  CASE code_type
    WHEN 'FEETYP' THEN (SELECT product_category_adw_key  from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` where product_category_cd  = 'MBR_FEES')  
    WHEN 'COMPCD' THEN (SELECT product_category_adw_key  from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` where product_category_cd  = 'RCC' )
  END AS product_category_adw_key  
  , concat(ifnull(code_type,''),'|', ifnull(code,'') ) as product_sku 
  , code as product_sku_key
  , (select Vendor_ADW_Key from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor` where vendor_cd =  'ACA' and vendor_typ = 'Membership' and actv_ind ='Y') as vendor_adw_key
  , code_desc as product_sku_desc
  , SAFE_CAST(last_upd_dt AS DATETIME)  as last_upd_dt
  , CAST('9999-12-31' AS datetime) effective_end_datetime
  , 'Y' as actv_ind
  , CURRENT_DATETIME() as integrate_insert_datetime
  , {{ dag_run.id }} as integrate_insert_batch_number
  , CURRENT_DATETIME() as integrate_update_datetime
  , {{ dag_run.id }} as integrate_update_batch_number
  , TO_BASE64(MD5(CONCAT(COALESCE(code_type,''),COALESCE(code_desc,'')))) as adw_row_hash 
FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_source` as source 
WHERE source.dupe_check=1 