CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_fact_customer_work_transformed`  AS

SELECT
 source.contact_adw_key
,DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) as integrate_snapshot_dt
,case when source.mbr_status_cd in ('A', 'P') then 'Y' else 'N' end as active_client_ind           -- Currently based on membership only (in future to be expanded for other business areas)
,safe_cast(null as STRING) as mbrs_active_donor
,source.mbrs_out_of_territory_cd as mbrs_territory_ind
,case when source.mbr_status_cd in ('A', 'P') then 'Y' else 'N' end as membership_actv_ind
,source.mbr_join_club_dt as mbrs_first_dt
,source.mbr_active_expiration_dt as mbrs_last_dt
,product.product_sku_desc as mbrs_coverage_level
,member.mbrs_mbr_cnt as mbrs_mbr_cnt
,safe_cast(null as STRING) as insurance_actv_ind
,safe_cast(null as INT64) as ins_lifetime_policy_cnt
,safe_cast(null as INT64) as ins_active_policy_cnt
,safe_cast(null as DATE) as ins_first_policy_dt
,safe_cast(null as DATE) as ins_latest_policy_dt
,safe_cast(null as STRING) as cor_actv_ind
,safe_cast(null as INT64) as cor_service_cnt
,safe_cast(null as DATE) as cor_first_service_dt
,safe_cast(null as DATE) as cor_latest_service_dt
,safe_cast(null as STRING) as retail_actv_ind
,safe_cast(null as INT64) as retail_client_service_cnt
,safe_cast(null as DATE) as retail_first_purchase_dt
,safe_cast(null as DATE) as retail_latest_purchase_dt
,safe_cast(null as STRING) as travel_actv_ind
,safe_cast(null as INT64) as tvl_booking_cnt
,safe_cast(null as DATE) as tvl_first_booking_dt
,safe_cast(null as DATE) as tvl_latest_booking_dt
,safe_cast(null as STRING) as financial_service_actv_ind
,safe_cast(null as INT64) as financial_service_client_cnt
,safe_cast(null as DATE) as financial_service_first_dt
,safe_cast(null as DATE) as financial_service_latest_dt
,TO_BASE64(MD5(CONCAT(ifnull(case when source.mbr_status_cd in ('A', 'P') then 'Y' else 'N' end,         --active_client_ind
              ''),'|',ifnull(null,                                                                    --mbrs_active_donor
              ''),'|',ifnull(source.mbrs_out_of_territory_cd,                                 --mbrs_territory_ind
              ''),'|',ifnull(case when source.mbr_status_cd in ('A', 'P') then 'Y' else 'N' end,         --membership_actv_ind
              ''),'|',ifnull(safe_cast(source.mbr_join_club_dt as STRING),                       --mbrs_first_dt
              ''),'|',ifnull(safe_cast(source.mbr_active_expiration_dt as STRING),               --mbrs_last_dt
              ''),'|',ifnull(product.product_sku_desc,                                         --mbrs_coverage_level
              ''),'|',ifnull(safe_cast(member.mbrs_mbr_cnt as STRING),                     --mbrs_mbr_cnt
              ''),'|',ifnull(null,                                                                    --insurance_actv_ind
              ''),'|',ifnull(null,                                                                    --ins_lifetime_policy_cnt
              ''),'|',ifnull(null,                                                                    --ins_active_policy_cnt
              ''),'|',ifnull(null,                                                                    --ins_first_policy_dt
              ''),'|',ifnull(null,                                                                    --ins_latest_policy_dt
              ''),'|',ifnull(null,                                                                    --cor_actv_ind
              ''),'|',ifnull(null,                                                                    --cor_service_cnt
              ''),'|',ifnull(null,                                                                    --cor_first_service_dt
              ''),'|',ifnull(null,                                                                    --cor_latest_service_dt
              ''),'|',ifnull(null,                                                                    --retail_actv_ind
              ''),'|',ifnull(null,                                                                    --retail_client_service_cnt
              ''),'|',ifnull(null,                                                                    --retail_first_purchase_dt
              ''),'|',ifnull(null,                                                                    --retail_latest_purchase_dt
              ''),'|',ifnull(null,                                                                    --travel_actv_ind
              ''),'|',ifnull(null,                                                                    --tvl_booking_cnt
              ''),'|',ifnull(null,                                                                    --tvl_first_booking_dt
              ''),'|',ifnull(null,                                                                    --tvl_latest_booking_dt
              ''),'|',ifnull(null,                                                                    --financial_service_actv_ind
              ''),'|',ifnull(null,                                                                    --financial_service_client_cnt
              ''),'|',ifnull(null,                                                                    --financial_service_first_dt
              ''),'|',ifnull(null,                                                                    --financial_service_latest_dt
              ''),'|',ifnull('Y',                                                                     --actv_ind
              '') ))) AS adw_row_hash
,'Y' as actv_ind
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_fact_customer_source` source
  left join (
             SELECT
               product_adw_key,
               product_sku_desc
             FROM
               `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`
             WHERE
               actv_ind= 'Y'
  ) product
  on source.product_adw_key = product.product_adw_key
  left join
      ( select
           contact_adw_key
          ,count(mbr_adw_key) as mbrs_mbr_cnt
        from
          `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`
        where actv_ind = 'Y'
		group by contact_adw_key
      ) member
   on source.contact_adw_key = member.contact_adw_key
where source.dupe_check = 1
;