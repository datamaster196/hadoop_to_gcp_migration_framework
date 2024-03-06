CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_work_transformed` AS
 SELECT
    membership.billing_cd AS mbrs_billing_cd,
    membership.membership_id AS mbrs_id ,
    membership.membership_ky AS mbrs_source_system_key,
    `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(trim(membership.address_line1),''), coalesce(trim(membership.address_line2),''), cast('' as string), coalesce(trim(membership.city),''), coalesce(trim(membership.state),''), coalesce(trim(membership.zip),''), coalesce(trim(membership.delivery_route),''), coalesce(trim(membership.country),'')) as address_adw_key,
	coalesce(trim(membership.address_line1),'') as address_line1,
	coalesce(trim(membership.address_line2),'') as address_line2,
	coalesce(trim(membership.city),'') as city,
	coalesce(trim(membership.state),'') as state,
	coalesce(trim(membership.zip),'') as zip,
	coalesce(trim(membership.delivery_route),'') as delivery_route,
	coalesce(trim(membership.country),'') as country,
    membership.membership_type_cd AS mbrs_typ_cd,
    membership.cancel_dt AS mbrs_cancel_dt,
    membership.status_dt AS mbrs_status_dtm,
    coalesce(phone.phone_adw_key, '-1') AS phone_adw_key,
    coalesce(product.product_adw_key, '-1') AS product_adw_key,
    coalesce(aca_office.aca_office_adw_key, '-1') as aca_office_adw_key,
    membership.status AS mbrs_status_cd,
    membership.billing_category_cd AS mbrs_billing_category_cd,
    'N' AS mbrs_purge_ind,
    membership.dues_cost_at AS mbrs_dues_cost_amt,
    membership.dues_adjustment_at AS mbrs_dues_adj_amt,
    membership.future_cancel_fl AS mbrs_future_cancel_ind,
    membership.future_cancel_dt AS mbrs_future_cancel_dt,
    membership.oot_fl AS mbrs_out_of_territory_cd,
    membership.address_change_dt AS mbrs_address_change_dtm,
    membership.adm_d2000_update_dt AS mbrs_cdx_export_update_dtm,
    membership.tier_ky AS mbrs_tier_key,
    membership.temp_addr_ind AS mbrs_temporary_address_ind,
    membership.previous_membership_id AS previous_club_mbrs_id ,
    membership.previous_club_cd AS previous_club_cd,
    membership.transfer_club_cd AS mbrs_transfer_club_cd,
    membership.transfer_dt AS mbrs_transfer_dtm,
    membership_crossref.previous_club_cd as mbrs_merged_previous_club_cd,
    membership_crossref.previous_membership_id as mbrs_mbrs_merged_previous_id ,
    membership_crossref.previous_membership_ky as mbrs_mbrs_merged_previous_key,
    membership.dont_solicit_fl AS mbrs_dn_solicit_ind,
    membership.bad_address_fl AS mbrs_bad_address_ind,
    membership.dont_send_pub_fl AS mbrs_dn_send_pubs_ind,
    membership.market_tracking_cd AS mbrs_market_tracking_cd,
    membership.salvage_fl AS mbrs_salvage_ind,
    membership.coverage_level_cd AS mbrs_coverage_level_code,
    membership.group_cd AS mbrs_group_cd,
    membership.ebill_fl AS mbrs_ebill_ind,
    membership.segmentation_cd AS mbrs_marketing_segmntn_cd,
    membership.promo_code AS mbrs_promo_cd,
    membership.phone_type AS mbrs_phone_typ_cd,
    coalesce(membership_code.ERS_CODE,'N') AS mbrs_ers_abuser_ind,
    coalesce(membership_code.SAL_CODE,'N') AS mbrs_salvaged_ind,
    coalesce(membership_code.SHMC_CODE,'N') AS mbrs_shadow_mcycle_ind,
    coalesce(membership_code.STDNT_CODE,'N') AS mbrs_student_ind,
    coalesce(membership_code.NSAL_CODE,'N') AS mbrs_dn_salvage_ind,
    coalesce(membership_code.NCOA_CODE,'N') AS mbrs_address_ncoa_update_ind,
    coalesce(membership_code.RET_CODE,'N') AS mbrs_assigned_retention_ind,
    coalesce(membership_code.NMSP_CODE,'N') AS mbrs_mbrs_no_promotion_ind,
    coalesce(membership_code.NINETY_CODE,'N') AS mbrs_dn_offer_plus_ind,
    coalesce(membership_code.AAAWON_CODE,'N') AS mbrs_aaawld_online_ed_ind,
    coalesce(membership_code.SHADOW_CODE,'N') AS mbrs_shadowrv_coverage_ind,
    coalesce(membership_code.SIX_CODE,'N') AS mbrs_heavy_ers_ind,
    CAST (membership.last_upd_dt AS datetime) AS last_upd_dt,
    membership.unapplied_at as mbrs_unapplied_amt,
    membership.advance_pay_at as mbrs_advance_payment_amt,
    membership.pp_fc_dt as payment_plan_future_cancel_dt,
    membership.pp_fc_fl as payment_plan_future_cancel_ind,
    membership.switch_primary_fl as switch_prim_ind,
    membership.suppress_bill_fl as supress_bill_ind,
    membership.offer_end_dt as offer_end_dt,
    membership.opp_fl as old_payment_plan_ind,
    membership.carryover_at as carry_over_amt,
    TO_BASE64(MD5(CONCAT(ifnull(membership.billing_cd,
            ''),'|',ifnull(membership.membership_id,
	        ''),'|',ifnull(coalesce(trim(membership.address_line1),''),
	        ''),'|',ifnull(coalesce(trim(membership.address_line2),''),
	        ''),'|',ifnull(coalesce(trim(membership.city),''),
	        ''),'|',ifnull(coalesce(trim(membership.state),''),
	        ''),'|',ifnull(coalesce(trim(membership.zip),''),
	        ''),'|',ifnull(coalesce(trim(membership.delivery_route),''),
	        ''),'|',ifnull(coalesce(trim(membership.country),''),
            ''),'|',ifnull(membership.membership_type_cd,
            ''),'|',ifnull(membership.cancel_dt,
            ''),'|',ifnull(membership.status_dt,
            ''),'|',ifnull(membership.status,
            ''),'|',ifnull(membership.billing_category_cd,
            ''),'|',ifnull(membership.dues_cost_at,
            ''),'|',ifnull(membership.dues_adjustment_at,
            ''),'|',ifnull(membership.future_cancel_fl,
            ''),'|',ifnull(membership.future_cancel_dt,
            ''),'|',ifnull(membership.oot_fl,
            ''),'|',ifnull(membership.address_change_dt,
            ''),'|',ifnull(membership.adm_d2000_update_dt,
            ''),'|',ifnull(membership.tier_ky,
            ''),'|',ifnull(membership.temp_addr_ind,
            ''),'|',ifnull(membership.previous_membership_id,
            ''),'|',ifnull(membership.previous_club_cd,
            ''),'|',ifnull(membership.transfer_club_cd,
            ''),'|',ifnull(membership.transfer_dt,
            ''),'|',ifnull(membership.dont_solicit_fl,
            ''),'|',ifnull(membership.bad_address_fl,
            ''),'|',ifnull(membership.dont_send_pub_fl,
            ''),'|',ifnull(membership.market_tracking_cd,
            ''),'|',ifnull(membership.salvage_fl,
            ''),'|',ifnull(membership.group_cd,
            ''),'|',ifnull(membership.ebill_fl,
            ''),'|',ifnull(membership.segmentation_cd,
            ''),'|',ifnull(membership.promo_code,
            ''),'|',ifnull(membership.phone_type,
            ''),'|',ifnull(phone.phone_adw_key,
          '-1'),'|',ifnull(product.product_adw_key,
          '-1'),'|',ifnull(aca_office.aca_office_adw_key,
          '-1'),'|',ifnull(coalesce(membership_code.ERS_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.SAL_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.SHMC_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.STDNT_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.NSAL_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.NCOA_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.RET_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.NMSP_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.NINETY_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.AAAWON_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.SHADOW_CODE,'N'),
            ''),'|',ifnull(coalesce(membership_code.SIX_CODE,'N'),
            ''),'|',ifnull(membership.unapplied_at,
            ''),'|',ifnull(membership.advance_pay_at,
            ''),'|',ifnull(membership.pp_fc_dt,
            ''),'|',ifnull(membership.pp_fc_fl,
            ''),'|',ifnull(membership.switch_primary_fl,
            ''),'|',ifnull(membership.suppress_bill_fl,
            ''),'|',ifnull(membership.offer_end_dt,
            ''),'|',ifnull(membership.opp_fl,
            ''),'|',ifnull(membership.carryover_at,
            '') ))) AS adw_row_hash
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_work_source` AS membership
  LEFT JOIN (
    SELECT
      membership_ky,
      MAX(CASE
          WHEN code='ERS' THEN 'Y'
        ELSE
        'N'
      END
        ) AS ERS_CODE,
      MAX(CASE
          WHEN code='SAL' THEN 'Y'
        ELSE
        'N'
      END
        ) AS SAL_CODE,
      MAX(CASE
          WHEN code='SHMC' THEN 'Y'
        ELSE
        'N'
      END
        ) AS SHMC_CODE,
      MAX(CASE
          WHEN code='STDNT' THEN 'Y'
        ELSE
        'N'
      END
        ) AS STDNT_CODE,
      MAX(CASE
          WHEN code='NSAL' THEN 'Y'
        ELSE
        'N'
      END
        ) AS NSAL_CODE,
      MAX(CASE
          WHEN code='NCOA' THEN 'Y'
        ELSE
        'N'
      END
        ) AS NCOA_CODE,
      MAX(CASE
          WHEN code='RET' THEN 'Y'
        ELSE
        'N'
      END
        ) AS RET_CODE,
      MAX(CASE
          WHEN code='NMSP' THEN 'Y'
        ELSE
        'N'
      END
        ) AS NMSP_CODE,
      MAX(CASE
          WHEN code='90' THEN 'Y'
        ELSE
        'N'
      END
        ) AS NINETY_CODE,
      MAX(CASE
          WHEN code='AAAWON' THEN 'Y'
        ELSE
        'N'
      END
        ) AS AAAWON_CODE,
      MAX(CASE
          WHEN code='SHADOW' THEN 'Y'
        ELSE
        'N'
      END
        ) AS SHADOW_CODE,
      MAX(CASE
          WHEN code='6' THEN 'Y'
        ELSE
        'N'
      END
        ) AS SIX_CODE
    FROM
      `{{ var.value.INGESTION_PROJECT }}.mzp.membership_code`
    GROUP BY
      1) AS membership_code
  ON
    membership.membership_ky = membership_code.membership_ky
    -- Take from Integration Layer
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone` AS phone
  ON
    membership.phone = phone.raw_phone_nbr and phone.phone_adw_key != '-1'
  LEFT JOIN
    (select product_sku_key,
            product_adw_key,
            ROW_NUMBER() over (PARTITION BY product_sku_key) DUPE_CHECK
      FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`
      WHERE actv_ind = 'Y' ) product
  ON
    membership.coverage_level_cd = product.product_sku_key and product.DUPE_CHECK=1
  LEFT JOIN
    `{{ var.value.INGESTION_PROJECT }}.mzp.branch` as branch
  ON membership.branch_ky = branch.branch_ky
    left join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` as aca_office
        on branch.branch_cd = aca_office.mbrs_branch_cd and aca_office.actv_ind = 'Y'
    left join (select membership_id,
                previous_club_cd,
                previous_membership_id,
                previous_membership_ky,
                ROW_NUMBER() OVER(PARTITION BY membership_id ORDER BY last_upd_dt DESC) AS dupe_check
               from `{{ var.value.INGESTION_PROJECT }}.mzp.membership_crossref`
              ) membership_crossref
                 on membership.membership_id=membership_crossref.membership_id and membership_crossref.dupe_check = 1

  WHERE
    membership.dupe_check=1