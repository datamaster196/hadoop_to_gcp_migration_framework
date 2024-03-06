------------------ Load Prep work ------

-- Transformations

CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_detail_work_transformed` AS
SELECT
  GENERATE_UUID() as fact_sales_detail_adw_key
 ,coalesce (fsh.sales_header_adw_key,'-1') as sales_header_adw_key
 ,posli.ofc_id as ofc_id
 ,posli.sale_id
 ,'-1' as product_adw_key
 ,'-1' as biz_line_adw_key
 ,safe_cast(posli.cust_recpt_ln_itm_id as int64) as line_item_nbr
 ,sku_itm_cat.inv_itm_nr as item_cd
 ,sku_itm_cat.inv_itm_desc as item_desc
 ,sku_itm_cat.inv_cat_cd as item_category_cd
 ,sku_itm_cat.inv_cat_desc as item_category_desc
 ,safe_cast(posli.cust_recpt_ln_itm_qty_ct as int64) as quantity_sold
 ,coalesce(safe_cast(posli.sale_prc as numeric),0) as unit_sale_price
 ,cast(null as numeric) as unit_labor_cost
 ,coalesce(safe_cast(posli.user_dec as numeric),0)/100 as unit_item_cost
 ,coalesce(safe_cast(posli.sale_prc as numeric),0) * coalesce(safe_cast(posli.cust_recpt_ln_itm_qty_ct as numeric),0) as extended_sale_price
 ,cast(null as numeric) as extended_labor_cost
 ,(coalesce(safe_cast(posli.user_dec as numeric),0)/100) * (coalesce(safe_cast(posli.cust_recpt_ln_itm_qty_ct as numeric),0)) as extended_item_cost
 ,safe_cast(posli.last_upd_dt as datetime) as effective_start_datetime
 ,safe_cast('9999-12-31' as datetime) as effective_end_datetime
 ,'Y' as actv_ind
 ,TO_BASE64(MD5(CONCAT(ifnull(trim(posli.sale_id),
            ''),'|',ifnull(trim(posli.ofc_id),
	        ''),'|',ifnull(coalesce(trim(posli.cust_recpt_ln_itm_id),''),
	        ''),'|',ifnull(coalesce(trim('POS'),''),
	        ''),'|',ifnull(coalesce(trim('-1'),''),
	        ''),'|',ifnull(coalesce(trim('-1'),''),
	        ''),'|',ifnull(coalesce(trim(sku_itm_cat.inv_itm_nr),''),
	        ''),'|',ifnull(coalesce(trim(sku_itm_cat.inv_itm_desc),''),
	        ''),'|',ifnull(coalesce(trim(sku_itm_cat.inv_cat_cd),''),
	        ''),'|',ifnull(coalesce(trim(sku_itm_cat.inv_cat_desc),''),
	        ''),'|',ifnull(coalesce(trim(posli.cust_recpt_ln_itm_qty_ct),''),
	        ''),'|',ifnull(coalesce(trim(posli.sale_prc),''),
	        ''),'|',ifnull(coalesce(trim(posli.user_dec),''),
            '') ))) AS adw_row_hash
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_detail_work_source` AS posli
     left join
 	 (select aca_office_adw_key, retail_sales_office_cd
 	    from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`
 	   where actv_ind = 'Y') acaoff
 	  on acaoff.retail_sales_office_cd = coalesce(cast(cast(trim(posli.ofc_id) as int64) as string),'')
     inner join
	 (select sales_header_adw_key, source_system, source_system_key, aca_office_adw_key
	    from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`
	   where actv_ind = 'Y') fsh
	  on fsh.source_system =  'POS'
	     and
	     fsh.aca_office_adw_key =  coalesce(acaoff.aca_office_adw_key, '-1')
	     and
	     fsh.source_system_key = coalesce(trim(posli.sale_id),'')
     left join
     (SELECT  sku.inv_sku_id, itm.inv_itm_nr, itm.inv_itm_desc, cat.inv_cat_cd, cat.inv_cat_desc
	    FROM (select inv_sku_id, inv_itm_id, row_number() over (partition by inv_sku_id order by null) as sku_dup_chk
		       from `{{ var.value.INGESTION_PROJECT }}.pos.inv_sku` where clb_cd = '212') AS sku
	          inner join
	          ( select inv_itm_id, inv_itm_nr, inv_itm_desc, inv_cat_cd, row_number() over (partition by inv_itm_id order by null) as itm_dup_chk
			    from `{{ var.value.INGESTION_PROJECT }}.pos.inv_itm` where clb_cd = '212') AS itm
			    on sku.inv_itm_id = itm.inv_itm_id and sku.sku_dup_chk = 1 and itm.itm_dup_chk = 1
			  inner join
	         (select inv_cat_cd, inv_cat_desc, row_number() over (partition by inv_cat_cd order by null) as cat_dup_chk
			   from `{{ var.value.INGESTION_PROJECT }}.pos.inv_cat` where clb_cd = '212') AS cat
			    on itm.inv_cat_cd = cat.inv_cat_cd and itm.itm_dup_chk = 1 and cat.cat_dup_chk = 1
	 ) sku_itm_cat
	 on posli.inv_sku_id = sku_itm_cat.inv_sku_id
  WHERE posli.dupe_check = 1

-------------------------- End of script ------
