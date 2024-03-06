------------------ Historical Load Prep work ------
-- Clean up work area
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_detail_work_source`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_detail_work_transformed`;

-- Source
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_detail_work_source` AS
SELECT
   source.sale_id,
   source.ofc_id,
   source.cust_recpt_ln_itm_id,
   source.inv_sku_id,
   source.cust_recpt_ln_itm_qty_ct,
   source.sale_prc,
   source.user_dec,
   source.last_update as last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY source.ofc_id, source.sale_id, source.cust_recpt_ln_itm_id ORDER BY source.last_update DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.pos.cust_recpt_ln_itm` AS source
  WHERE Exists
    ( select 1 from `{{ var.value.INGESTION_PROJECT }}.pos.cust_recpt`  hdr
       where trim(source.clb_cd)  = trim(hdr.clb_cd)
         and  trim(source.ofc_id)  = trim(hdr.ofc_id)
         and  trim(source.sale_id) = trim(hdr.sale_id)
    )
    AND
    CAST(source.last_update AS datetime) > (
    SELECT
      MAX(effective_start_datetime)
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`)
   ;

-------------------------- End of script ------