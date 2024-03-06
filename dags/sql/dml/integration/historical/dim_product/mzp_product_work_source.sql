CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_source`  as 
SELECT 
  code_type
  , code
  , code_desc
  , last_upd_dt
  , ROW_NUMBER() OVER(PARTITION BY source.code_ky, source.last_upd_dt ORDER BY  NULL DESC) AS dupe_check 
FROM
`{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes` as source 
WHERE code_type in  ( 'FEETYP', 'COMPCD') 