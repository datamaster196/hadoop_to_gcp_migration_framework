CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_source`  as 
SELECT 
  code_type
  , code
  , code_desc
  , last_upd_dt
  , ROW_NUMBER() OVER(PARTITION BY source.code_ky ORDER BY source.last_upd_dt DESC) AS dupe_check
FROM
`{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes` as source 
WHERE code_type in  ( 'FEETYP', 'COMPCD')  and 
  CAST(source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`)