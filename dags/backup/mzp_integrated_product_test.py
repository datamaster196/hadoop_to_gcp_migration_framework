from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_payment_summary_incremental_test3"

default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'start_date'      : datetime(2019, 8, 22),
    'email'           : ['airflow@example.com'],
    'catchup'         : False,
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=5),
}

######################################################################
# Load Membership Historical set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

membership_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_source` AS
SELECT
  membership_source.billing_cd,
  membership_source.membership_id,
  membership_source.membership_ky,
  membership_source.address_line1,
  membership_source.address_line2,
  membership_source.city,
  membership_source.state,
  membership_source.zip,
  membership_source.country,
  membership_source.membership_type_cd,
  membership_source.cancel_dt,
  membership_source.status_dt,
  membership_source.phone,
  membership_source.status,
  membership_source.billing_category_cd,
  membership_source.dues_cost_at,
  membership_source.dues_adjustment_at,
  membership_source.future_cancel_fl,
  membership_source.future_cancel_dt,
  membership_source.oot_fl,
  membership_source.address_change_dt,
  membership_source.adm_d2000_update_dt,
  membership_source.tier_ky,
  membership_source.temp_addr_ind,
  membership_source.previous_membership_id,
  membership_source.previous_club_cd,
  membership_source.transfer_club_cd,
  membership_source.transfer_dt,
  membership_source.dont_solicit_fl,
  membership_source.bad_address_fl,
  membership_source.dont_send_pub_fl,
  membership_source.market_tracking_cd,
  membership_source.salvage_fl,
  membership_source.coverage_level_cd,
  membership_source.group_cd,
--  membership_source.coverage_level_ky,
  membership_source.ebill_fl,
  membership_source.segmentation_cd,
  membership_source.promo_code,
  membership_source.phone_type,
  membership_source.branch_ky,  
  membership_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY membership_source.membership_id ORDER BY membership_source.last_upd_dt DESC) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.membership` AS membership_source
WHERE
  CAST(membership_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` AS
  SELECT
    membership.billing_cd AS membership_billing_code,
    membership.membership_id AS membership_identifier,
    membership.membership_ky AS membership_source_system_key,
    TO_BASE64(MD5(CONCAT(ifnull(membership.address_line1,
            ''),'|',ifnull(membership.address_line2,
            ''),'|',ifnull(membership.city,
            ''),'|',ifnull(membership.state,
            ''),'|',ifnull(membership.zip,
            ''),'|',ifnull(membership.country,
            '')))) AS address_adw_key,
    membership.membership_type_cd AS membership_type_code,
    membership.cancel_dt AS membership_cancel_date,
    membership.status_dt AS membership_status_datetime,
    coalesce(phone.phone_adw_key, '-1') AS phone_adw_key,
    coalesce(product.product_adw_key, '-1') AS product_adw_key,		
    coalesce(aca_office.aca_office_adw_key, '-1') as aca_office_adw_key,
    membership.status AS membership_status_code,
    membership.billing_category_cd AS membership_billing_category_code,
    'N' AS membership_purge_indicator,
    membership.dues_cost_at AS membership_dues_cost_amount,
    membership.dues_adjustment_at AS membership_dues_adjustment_amount,
    membership.future_cancel_fl AS membership_future_cancel_indicator,
    membership.future_cancel_dt AS membership_future_cancel_date,
    membership.oot_fl AS membership_out_of_territory_code,
    membership.address_change_dt AS membership_address_change_datetime,
    membership.adm_d2000_update_dt AS membership_cdx_export_update_datetime,
    membership.tier_ky AS membership_tier_key,
    membership.temp_addr_ind AS membership_temporary_address_indicator,
    membership.previous_membership_id AS previous_club_membership_identifier,
    membership.previous_club_cd AS previous_club_code,
    membership.transfer_club_cd AS membership_transfer_club_code,
    membership.transfer_dt AS membership_transfer_datetime,
    membership_crossref.previous_club_cd as merged_membership_previous_club_code,
    membership_crossref.previous_membership_id as merged_member_previous_membership_identifier,
    membership_crossref.previous_membership_ky as merged_member_previous_membership_key,
    membership.dont_solicit_fl AS membership_dn_solicit_indicator,
    membership.bad_address_fl AS membership_bad_address_indicator,
    membership.dont_send_pub_fl AS membership_dn_send_publications_indicator,
    membership.market_tracking_cd AS membership_market_tracking_code,
    membership.salvage_fl AS membership_salvage_flag,
    membership.coverage_level_cd AS membership_coverage_level_code,
    membership.group_cd AS membership_group_code,
 --   membership.coverage_level_ky AS membership_coverage_level_key,
    membership.ebill_fl AS membership_ebill_indicator,
    membership.segmentation_cd AS membership_marketing_segmentation_code,
    membership.promo_code AS membership_promo_code,
    membership.phone_type AS membership_phone_type_code,
    membership_code.ERS_CODE AS membership_ers_abuser_indicator,
    membership_code.DSP_CODE AS membership_dn_send_aaaworld_indicator,
    membership_code.SAL_CODE AS membership_salvaged_indicator,
    membership_code.SHMC_CODE AS membership_shadow_motorcycle_indicator,
    membership_code.STDNT_CODE AS membership_student_indicator,
    membership_code.NSAL_CODE AS membership_dn_salvage_indicator,
    membership_code.NCOA_CODE AS membership_address_updated_by_ncoa_indicator,
    membership_code.RET_CODE AS membership_assigned_retention_indicator,
    membership_code.NMSP_CODE AS membership_no_membership_promotion_mailings_indicator,
    membership_code.DNTM_CODE AS membership_dn_telemarket_indicator,
    membership_code.NINETY_CODE AS membership_dn_offer_plus_indicator,
    membership_code.AAAWON_CODE AS membership_receive_aaaworld_online_edition_indicator,
    membership_code.SHADOW_CODE AS membership_shadowrv_coverage_indicator,
    membership_code.DNDM_CODE AS membership_dn_direct_mail_solicit_indicator,
    membership_code.SIX_CODE AS membership_heavy_ers_indicator,
    CAST (membership.last_upd_dt AS datetime) AS last_upd_dt,
    TO_BASE64(MD5(CONCAT(ifnull(membership.billing_cd,
            ''),'|',ifnull(membership.membership_id,
            ''),'|',ifnull(membership.address_line1,
            ''),'|',ifnull(membership.address_line2,
            ''),'|',ifnull(membership.city,
            ''),'|',ifnull(membership.state,
            ''),'|',ifnull(membership.zip,
            ''),'|',ifnull(membership.country,
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
            ''),'|',ifnull(membership.coverage_level_cd,
            ''),'|',ifnull(membership.group_cd,
           -- ''),'|',ifnull(membership.coverage_level_ky,
            ''),'|',ifnull(membership.ebill_fl,
            ''),'|',ifnull(membership.segmentation_cd,
            ''),'|',ifnull(membership.promo_code,
            ''),'|',ifnull(membership.phone_type,
            ''),'|',ifnull(CAST(membership_code.ERS_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.DSP_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SAL_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SHMC_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.STDNT_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NSAL_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NCOA_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.RET_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NMSP_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.DNTM_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NINETY_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.AAAWON_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SHADOW_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.DNDM_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SIX_CODE AS string),
            '') ))) AS adw_row_hash
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_source` AS membership
  LEFT JOIN (
    SELECT
      membership_ky,
      MAX(CASE
          WHEN code='ERS' THEN 1
        ELSE
        0
      END
        ) AS ERS_CODE,
      MAX(CASE
          WHEN code='DSP' THEN 1
        ELSE
        0
      END
        ) AS DSP_CODE,
      MAX(CASE
          WHEN code='SAL' THEN 1
        ELSE
        0
      END
        ) AS SAL_CODE,
      MAX(CASE
          WHEN code='SHMC' THEN 1
        ELSE
        0
      END
        ) AS SHMC_CODE,
      MAX(CASE
          WHEN code='STDNT' THEN 1
        ELSE
        0
      END
        ) AS STDNT_CODE,
      MAX(CASE
          WHEN code='NSAL' THEN 1
        ELSE
        0
      END
        ) AS NSAL_CODE,
      MAX(CASE
          WHEN code='NCOA' THEN 1
        ELSE
        0
      END
        ) AS NCOA_CODE,
      MAX(CASE
          WHEN code='RET' THEN 1
        ELSE
        0
      END
        ) AS RET_CODE,
      MAX(CASE
          WHEN code='NMSP' THEN 1
        ELSE
        0
      END
        ) AS NMSP_CODE,
      MAX(CASE
          WHEN code='DNTM' THEN 1
        ELSE
        0
      END
        ) AS DNTM_CODE,
      MAX(CASE
          WHEN code='90' THEN 1
        ELSE
        0
      END
        ) AS NINETY_CODE,
      MAX(CASE
          WHEN code='AAAWON' THEN 1
        ELSE
        0
      END
        ) AS AAAWON_CODE,
      MAX(CASE
          WHEN code='SHADOW' THEN 1
        ELSE
        0
      END
        ) AS SHADOW_CODE,
      MAX(CASE
          WHEN code='DNDM' THEN 1
        ELSE
        0
      END
        ) AS DNDM_CODE,
      MAX(CASE
          WHEN code='6' THEN 1
        ELSE
        0
      END
        ) AS SIX_CODE
    FROM
      `{iu.INGESTION_PROJECT}.mzp.membership_code`
    GROUP BY
      1) AS membership_code
  ON
    membership.membership_ky = membership_code.membership_ky
    -- Take from Integration Layer
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone` AS phone
  ON
    membership.phone = phone.raw_phone_number
  LEFT JOIN 
    `{int_u.INTEGRATION_PROJECT}.adw.dim_product` AS product
  ON
    membership.coverage_level_cd = product.product_sku and product.active_indicator = 'Y'
  LEFT JOIN
    `{iu.INGESTION_PROJECT}.mzp.branch` as branch
  ON membership.branch_ky = branch.branch_ky
    left join `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` as aca_office
    	on branch.branch_cd = aca_office.membership_branch_code
    left join (select 
		        membership_id,
                previous_club_cd,
                previous_membership_id,
                previous_membership_ky,              
                ROW_NUMBER() OVER(PARTITION BY membership_id ORDER BY last_upd_dt DESC) AS dupe_check
               from `{iu.INGESTION_PROJECT}.mzp.membership_crossref`
              ) membership_crossref
     	        on membership.membership_id=membership_crossref.membership_id and membership_crossref.dupe_check = 1
  WHERE
    membership.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_stage` AS
  SELECT
    COALESCE(target.membership_adw_key,
      GENERATE_UUID()) membership_adw_key,
    source.address_adw_key,
    CAST (source.phone_adw_key AS string) phone_adw_key,
    CAST (source.product_adw_key AS string) product_adw_key,	
    CAST (source.aca_office_adw_key AS string) aca_office_adw_key,
    CAST (source.membership_source_system_key AS int64) membership_source_system_key,
    source.membership_identifier,
    source.membership_status_code,
    source.membership_billing_code,
    CAST(source.membership_status_datetime AS datetime) membership_status_datetime,
    CAST(substr (source.membership_cancel_date,
        0,
        10) AS date) membership_cancel_date,
    source.membership_billing_category_code,
    CAST(source.membership_dues_cost_amount AS numeric) membership_dues_cost_amount,
    CAST(source.membership_dues_adjustment_amount AS numeric) membership_dues_adjustment_amount,
    source.membership_future_cancel_indicator,
    CAST(substr (source.membership_future_cancel_date,
        0,
        10) AS date) membership_future_cancel_date,
    source.membership_out_of_territory_code,
    CAST(source.membership_address_change_datetime AS datetime) membership_address_change_datetime,
    CAST(source.membership_cdx_export_update_datetime AS datetime) membership_cdx_export_update_datetime,
    CAST(source.membership_tier_key AS float64) membership_tier_key,
    source.membership_temporary_address_indicator,
    source.previous_club_membership_identifier,
    source.previous_club_code,
    source.membership_transfer_club_code,
    CAST(source.membership_transfer_datetime AS datetime) membership_transfer_datetime,
    CAST(source.merged_membership_previous_club_code AS string) merged_membership_previous_club_code,
    CAST(source.merged_member_previous_membership_identifier AS string) merged_member_previous_membership_identifier,
    safe_cast(source.merged_member_previous_membership_key as int64) as merged_member_previous_membership_key,
    source.membership_dn_solicit_indicator,
    source.membership_bad_address_indicator,
    source.membership_dn_send_publications_indicator,
    source.membership_market_tracking_code,
    source.membership_salvage_flag,
    CAST(source.membership_salvaged_indicator AS string) membership_salvaged_indicator,
    CAST(source.membership_dn_salvage_indicator AS string) membership_dn_salvage_indicator,
    --source.membership_coverage_level_code,
    --CAST(source.membership_coverage_level_key AS int64) membership_coverage_level_key,
    source.membership_group_code,
    source.membership_type_code,
    source.membership_ebill_indicator,
    source.membership_marketing_segmentation_code,
    source.membership_promo_code,
    source.membership_phone_type_code,
    CAST(source.membership_ers_abuser_indicator AS string) membership_ers_abuser_indicator,
    CAST(source.membership_dn_send_aaaworld_indicator AS string) membership_dn_send_aaaworld_indicator,
    CAST(source.membership_shadow_motorcycle_indicator AS string) membership_shadow_motorcycle_indicator,
    CAST(source.membership_student_indicator AS string) membership_student_indicator,
    CAST(source.membership_address_updated_by_ncoa_indicator AS string) membership_address_updated_by_ncoa_indicator,
    CAST(source.membership_assigned_retention_indicator AS string) membership_assigned_retention_indicator,
    CAST(source.membership_no_membership_promotion_mailings_indicator AS string) membership_no_membership_promotion_mailings_indicator,
    CAST(source.membership_dn_telemarket_indicator AS string) membership_dn_telemarket_indicator,
    CAST(source.membership_dn_offer_plus_indicator AS string) membership_dn_offer_plus_indicator,
    CAST(source.membership_receive_aaaworld_online_edition_indicator AS string) membership_receive_aaaworld_online_edition_indicator,
    CAST(source.membership_shadowrv_coverage_indicator AS string) membership_shadowrv_coverage_indicator,
    CAST(source.membership_dn_direct_mail_solicit_indicator AS string) membership_dn_direct_mail_solicit_indicator,
    CAST(source.membership_heavy_ers_indicator AS string) membership_heavy_ers_indicator,
    CAST(source.membership_purge_indicator AS string) membership_purge_indicator,
    last_upd_dt AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership` target
  ON
    (source.membership_identifier=target.membership_identifier
      AND target.active_indicator='Y')
  WHERE
    target.membership_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
  UNION ALL
  SELECT
    target.membership_adw_key,
    target.address_adw_key,
    target.phone_adw_key,
    target.product_adw_key,	
    target.aca_office_adw_key,
    target.membership_source_system_key,
    target.membership_identifier,
    target.membership_status_code,
    target.membership_billing_code,
    target.membership_status_datetime,
    target.membership_cancel_date,
    source.membership_billing_category_code,
    target.membership_dues_cost_amount,
    target.membership_dues_adjustment_amount,
    target.membership_future_cancel_indicator,
    target.membership_future_cancel_date,
    target.membership_out_of_territory_code,
    target.membership_address_change_datetime,
    target.membership_cdx_export_update_datetime,
    target.membership_tier_key,
    target.membership_temporary_address_indicator,
    target.previous_club_membership_identifier,
    target.previous_club_code,
    target.membership_transfer_club_code,
    target.membership_transfer_datetime,
    target.merged_membership_previous_club_code,
    target.merged_member_previous_membership_identifier,
    target.merged_member_previous_membership_key,
    target.membership_dn_solicit_indicator,
    target.membership_bad_address_indicator,
    target.membership_dn_send_publications_indicator,
    target.membership_market_tracking_code,
    target.membership_salvage_flag,
    target.membership_salvaged_indicator,
    target.membership_dn_salvage_indicator,
    --target.membership_coverage_level_code,
    --target.membership_coverage_level_key,
    target.membership_group_code,
    target.membership_type_code,
    target.membership_ebill_indicator,
    target.membership_marketing_segmentation_code,
    target.membership_promo_code,
    target.membership_phone_type_code,
    target.membership_ers_abuser_indicator,
    target.membership_dn_send_aaaworld_indicator,
    target.membership_shadow_motorcycle_indicator,
    target.membership_student_indicator,
    target.membership_address_updated_by_ncoa_indicator,
    target.membership_assigned_retention_indicator,
    target.membership_no_membership_promotion_mailings_indicator,
    target.membership_dn_telemarket_indicator,
    target.membership_dn_offer_plus_indicator,
    target.membership_receive_aaaworld_online_edition_indicator,
    target.membership_shadowrv_coverage_indicator,
    target.membership_dn_direct_mail_solicit_indicator,
    target.membership_heavy_ers_indicator,
    target.membership_purge_indicator,
    target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 day) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership` target
  ON
    (source.membership_identifier=target.membership_identifier
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash
    
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership
membership_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_stage` b
  ON
    (a.membership_adw_key = b.membership_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( 
    membership_adw_key, 
    address_adw_key, 
    phone_adw_key, 
    product_adw_key, 	
    aca_office_adw_key, 
    membership_source_system_key, 
    membership_identifier, 
    membership_status_code, 
    membership_billing_code, 
    membership_status_datetime, 
    membership_cancel_date, 
    membership_billing_category_code, 
    membership_dues_cost_amount, 
    membership_dues_adjustment_amount, 
    membership_future_cancel_indicator, 
    membership_future_cancel_date, 
    membership_out_of_territory_code, 
    membership_address_change_datetime, 
    membership_cdx_export_update_datetime, 
    membership_tier_key, 
    membership_temporary_address_indicator, 
    previous_club_membership_identifier, 
    previous_club_code, 
    membership_transfer_club_code, 
    membership_transfer_datetime, 
    merged_membership_previous_club_code, 
    merged_member_previous_membership_identifier, 
    merged_member_previous_membership_key, 
    membership_dn_solicit_indicator, 
    membership_bad_address_indicator, 
    membership_dn_send_publications_indicator, 
    membership_market_tracking_code, 
    membership_salvage_flag, 
    membership_salvaged_indicator, 
    membership_dn_salvage_indicator, 
  --  membership_coverage_level_code, 
  --  membership_coverage_level_key, 
    membership_group_code, 
    membership_type_code, 
    membership_ebill_indicator, 
    membership_marketing_segmentation_code, 
    membership_promo_code, 
    membership_phone_type_code, 
    membership_ers_abuser_indicator, 
    membership_dn_send_aaaworld_indicator, 
    membership_shadow_motorcycle_indicator, 
    membership_student_indicator, 
    membership_address_updated_by_ncoa_indicator, 
    membership_assigned_retention_indicator, 
    membership_no_membership_promotion_mailings_indicator, 
    membership_dn_telemarket_indicator, 
    membership_dn_offer_plus_indicator, 
    membership_receive_aaaworld_online_edition_indicator, 
    membership_shadowrv_coverage_indicator, 
    membership_dn_direct_mail_solicit_indicator, 
    membership_heavy_ers_indicator, 
    membership_purge_indicator, 
    effective_start_datetime, 
    effective_end_datetime, 
    active_indicator, 
    adw_row_hash, 
    integrate_insert_datetime, 
    integrate_insert_batch_number, 
    integrate_update_datetime, 
    integrate_update_batch_number ) VALUES (
    b.membership_adw_key,
    b.address_adw_key,
    b.phone_adw_key,
    b.product_adw_key,	
    b.aca_office_adw_key,
    b.membership_source_system_key,
    b.membership_identifier,
    b.membership_status_code,
    b.membership_billing_code,
    b.membership_status_datetime,
    b.membership_cancel_date,
    b.membership_billing_category_code,
    b.membership_dues_cost_amount,
    b.membership_dues_adjustment_amount,
    b.membership_future_cancel_indicator,
    b.membership_future_cancel_date,
    b.membership_out_of_territory_code,
    b.membership_address_change_datetime,
    b.membership_cdx_export_update_datetime,
    b.membership_tier_key,
    b.membership_temporary_address_indicator,
    b.previous_club_membership_identifier,
    b.previous_club_code,
    b.membership_transfer_club_code,
    b.membership_transfer_datetime,
    b.merged_membership_previous_club_code,
    b.merged_member_previous_membership_identifier,
    b.merged_member_previous_membership_key,
    b.membership_dn_solicit_indicator,
    b.membership_bad_address_indicator,
    b.membership_dn_send_publications_indicator,
    b.membership_market_tracking_code,
    b.membership_salvage_flag,
    b.membership_salvaged_indicator,
    b.membership_dn_salvage_indicator,
  --  b.membership_coverage_level_code,
  --  b.membership_coverage_level_key,
    b.membership_group_code,
    b.membership_type_code,
    b.membership_ebill_indicator,
    b.membership_marketing_segmentation_code,
    b.membership_promo_code,
    b.membership_phone_type_code,
    b.membership_ers_abuser_indicator,
    b.membership_dn_send_aaaworld_indicator,
    b.membership_shadow_motorcycle_indicator,
    b.membership_student_indicator,
    b.membership_address_updated_by_ncoa_indicator,
    b.membership_assigned_retention_indicator,
    b.membership_no_membership_promotion_mailings_indicator,
    b.membership_dn_telemarket_indicator,
    b.membership_dn_offer_plus_indicator,
    b.membership_receive_aaaworld_online_edition_indicator,
    b.membership_shadowrv_coverage_indicator,
    b.membership_dn_direct_mail_solicit_indicator,
    b.membership_heavy_ers_indicator,
    b.membership_purge_indicator,
    b.effective_start_datetime,
    b.effective_end_datetime,
    b.active_indicator,
    b.adw_row_hash,
    b.integrate_insert_datetime,
    b.integrate_insert_batch_number,
    b.integrate_update_datetime,
    b.integrate_update_batch_number 
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
"""
####################################################################################################################################
# Member
####################################################################################################################################

#################################
# Member - Stage Load
#################################
# Member


member_work_transformed = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.adw_member_stage_hist` as

SELECT *
FROM
(SELECT *,
ROW_NUMBER() OVER (PARTITION BY member_associate_identifier,member_identifier,member_source_key,HASH_VALUE ORDER BY effective_start_date) AS DUP_CHECK
FROM
(
SELECT
customer_adw_key,
member_adw_key,
sales_agent_adw_key,
membership_solicitation_adw_key,
member_status_code,
member_check_digit_number,
member_associate_identifier,
member_identifier,
member_source_key,
member_customer_identifier,
member_salutation_name,
member_first_name,
member_middle_name,
member_last_name,
member_suffix_name,
member_gender_code,
member_birth_date,
member_email_name,
member_renew_method_code,
member_commission_code,
member_source_of_sale_code,
member_solicitation_code,
member_associate_relation_code,
member_join_club_date,
member_join_aaa_date,
member_cancel_date,
member_status_datetime,
member_billing_category_code,
member_billing_code,
member_type_code,
member_expiration_date,
member_card_expiration_date,
member_reason_joined_code,
member_do_not_renew_indicator,
member_future_cancel_date,
member_future_cancel_indicator,
member_free_member_indicator,
member_previous_membership_identifier,
member_previous_club_code,
member_merged_member_key,
member_merged_club_code,
member_merged_membership_identifier,
member_merged_associate_identifier,
member_merged_check_digit_number,
member_merged_member16_identifier,
member_name_change_datetime,
member_adm_email_changed_datetime,
member_bad_email_indicator,
member_email_optout_indicator,
member_web_last_login_datetime,
member_active_expiration_date,
member_dn_text_indicator,
member_duplicate_email_indicator,
member_dn_call_indicator,
member_no_email_indicator,
member_dn_mail_indicator,
member_dnaf_email_indicator,
member_dn_email_indicator,
member_refused_give_email_indicator,
effective_start_date,
MD5(CONCAT(COALESCE(member_status_code,''),
COALESCE(member_check_digit_number,''),
COALESCE(member_customer_identifier,''),
COALESCE(member_salutation_name,''),
COALESCE(member_first_name,''),
COALESCE(member_middle_name,''),
COALESCE(member_last_name,''),
COALESCE(member_suffix_name,''),
COALESCE(member_gender_code,''),
COALESCE(member_birth_date,''),
COALESCE(member_email_name,''),
COALESCE(member_renew_method_code,''),
COALESCE(member_commission_code,''),
COALESCE(member_source_of_sale_code,''),
COALESCE(member_solicitation_code,''),
COALESCE(member_associate_relation_code,''),
COALESCE(member_join_club_date,''),
COALESCE(member_join_aaa_date,''),
COALESCE(member_cancel_date,''),
COALESCE(member_status_datetime,''),
COALESCE(member_billing_category_code,''),
COALESCE(member_billing_code,''),
COALESCE(member_type_code,''),
COALESCE(member_expiration_date,''),
COALESCE(member_card_expiration_date,''),
COALESCE(member_reason_joined_code,''),
COALESCE(member_do_not_renew_indicator,''),
COALESCE(member_future_cancel_date,''),
COALESCE(member_future_cancel_indicator,''),
COALESCE(member_free_member_indicator,''),
COALESCE(member_previous_membership_identifier,''),
COALESCE(member_previous_club_code,''),
COALESCE(member_merged_member_key,' '),
COALESCE(member_merged_club_code,' '),
COALESCE(member_merged_membership_identifier,' '),
COALESCE(member_merged_associate_identifier,' '),
COALESCE(member_merged_check_digit_number,' '),
COALESCE(member_merged_member16_identifier,' '),
COALESCE(member_name_change_datetime,''),
COALESCE(member_adm_email_changed_datetime,''),
COALESCE(member_bad_email_indicator,''),
COALESCE(member_email_optout_indicator,''),
COALESCE(member_web_last_login_datetime,''),
COALESCE(member_active_expiration_date,''),
--COALESCE(member_activation_date,''),
COALESCE(member_dn_text_indicator,''),
COALESCE(member_duplicate_email_indicator,''),
COALESCE(member_dn_call_indicator,''),
COALESCE(member_no_email_indicator,''),
COALESCE(member_dn_mail_indicator,''),
COALESCE(member_dnaf_email_indicator,''),
COALESCE(member_dn_email_indicator,''),
COALESCE(member_refused_give_email_indicator,''),
COALESCE(SAFE_CAST(effective_start_date AS STRING),'')
)) AS HASH_VALUE
FROM
(SELECT 
adw_memship.Membership_adw_key as membership_adw_key,
cust_src_key.customer_adw_key as customer_adw_key,
GENERATE_UUID() as member_adw_key,
'' as sales_agent_adw_key,
'' as membership_solicitation_adw_key,
mem.status as member_status_code,
mem.check_digit_nr as member_check_digit_number,
mem.associate_id as member_associate_identifier,
mem.membership_id as member_identifier,
mem.member_ky as member_source_key,
mem.customer_id as member_customer_identifier,
mem.salutation as member_salutation_name,
mem.first_name as member_first_name,
mem.middle_name as member_middle_name,
mem.last_name as member_last_name,
mem.name_suffix as member_suffix_name,
mem.gender as member_gender_code,
mem.birth_dt as member_birth_date,
mem.email as member_email_name,
mem.renew_method_cd as member_renew_method_code,
mem.commission_cd as member_commission_code,
mem.source_of_sale as member_source_of_sale_code,
mem.solicitation_cd as member_solicitation_code,
mem.associate_relation_cd as member_associate_relation_code,
mem.join_club_dt as member_join_club_date,
mem.join_aaa_dt as member_join_aaa_date,
mem.cancel_dt as member_cancel_date,
mem.status_dt as member_status_datetime,
mem.billing_category_cd as member_billing_category_code,
mem.billing_cd as member_billing_code,
mem.member_type_cd as member_type_code,
mem.member_expiration_dt as member_expiration_date,
mem.member_card_expiration_dt as member_card_expiration_date,
mem.reason_joined as member_reason_joined_code,
mem.do_not_renew_fl as member_do_not_renew_indicator,
mem.future_cancel_dt as member_future_cancel_date,
mem.future_cancel_fl as member_future_cancel_indicator,
mem.free_member_fl as member_free_member_indicator,
mem.previous_membership_id as member_previous_membership_identifier,
mem.previous_club_cd as member_previous_club_code,
crossref.previous_member_ky as member_merged_member_key,
crossref.previous_club_cd as member_merged_club_code,
crossref.previous_membership_id as member_merged_membership_identifier,
crossref.previous_associate_id as member_merged_associate_identifier,
crossref.previous_check_digit_nr as member_merged_check_digit_number,
crossref.previous_member16_id as member_merged_member16_identifier,
mem.name_change_dt as member_name_change_datetime,
mem.adm_email_changed_dt as member_adm_email_changed_datetime,
mem.bad_email_fl as member_bad_email_indicator,
mem.email_optout_fl as member_email_optout_indicator,
mem.web_last_login_dt as member_web_last_login_datetime,
mem.active_expiration_dt as member_active_expiration_date,
--mem.activation_dt as member_activation_date,
CASE WHEN code.CODE='DNT'
      THEN '1'
      ELSE '0'
      END AS member_dn_text_indicator,
 CASE WHEN code.CODE='DEM'
      THEN '1'
      ELSE '0'
      END AS member_duplicate_email_indicator,
 CASE WHEN code.CODE='DNC'
      THEN '1'
      ELSE '0'
      END AS member_dn_call_indicator,  
 CASE WHEN code.CODE='NOE'
      THEN '1'
      ELSE '0'
      END AS member_no_email_indicator,
 CASE WHEN code.CODE='DNM'
      THEN '1'
      ELSE '0'
      END AS member_dn_mail_indicator,
 CASE WHEN code.CODE='DNAE'
      THEN '1'
      ELSE '0'
      END AS member_dnaf_email_indicator,
 CASE WHEN code.CODE='DNE'
      THEN '1'
      ELSE '0'
      END AS member_dn_email_indicator,
 CASE WHEN code.CODE='RFE'
      THEN '1'
      ELSE '0'
      END AS member_refused_give_email_indicator,
DATE(SAFE_CAST(mem.last_upd_dt AS DATETIME)) AS effective_start_date
FROM `{iu.INGESTION_PROJECT}.mzp.member_hist` mem
 left outer join 
 `{int_u.INTEGRATION_PROJECT}.membership.membership` adw_memship
 on mem.membership_ky=adw_memship.membership_source_key
left outer join
`{int_u.INTEGRATION_PROJECT}.adw.customer_source_key` cust_src_key
on mem.member_ky=cust_src_key.customer_source_1_key and cust_src_key.customer_key_type_name='member_key'
left outer join
`{iu.INGESTION_PROJECT}.mzp.member_code` code
on code.member_ky=mem.member_ky
left outer join
`{iu.INGESTION_PROJECT}.mzp.member_crossref` crossref
on crossref.member_ky=mem.member_ky
and crossref.membership_id=mem.membership_id
and crossref.associate_id =mem.associate_id )
) )
WHERE DUP_CHECK=1

"""

#################################
# Member - Post Stage Load - Insert
#################################
# Member

member_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.adw.adw_member` 

(
--membership_adw_key,
customer_adw_key,
member_adw_key,
sales_agent_adw_key,
membership_solicitation_adw_key,
member_status_code,
member_check_digit_number,
member_associate_identifier,
member_identifier,
member_source_key,
member_customer_identifier,
member_salutation_name,
member_first_name,
member_middle_name,
member_last_name,
member_suffix_name,
member_gender_code,
member_birth_date,
member_email_name,
member_renew_method_code,
member_commission_code,
member_source_of_sale_code,
member_solicitation_code,
member_associate_relation_code,
member_join_club_date,
member_join_aaa_date,
member_cancel_date,
member_status_datetime,
member_billing_category_code,
member_billing_code,
member_type_code,
member_expiration_date,
member_card_expiration_date,
member_reason_joined_code,
member_do_not_renew_indicator,
member_future_cancel_date,
member_future_cancel_indicator,
member_free_member_indicator,
member_previous_membership_identifier,
member_previous_club_code,
member_merged_member_key,
member_merged_club_code,
member_merged_membership_identifier,
member_merged_associate_identifier,
member_merged_check_digit_number,
member_merged_member16_identifier,
member_name_change_datetime,
member_adm_email_changed_datetime,
member_bad_email_indicator,
member_email_optout_indicator,
member_web_last_login_datetime,
member_active_expiration_date,
--member_activation_date,
member_dn_text_indicator,
member_duplicate_email_indicator,
member_dn_call_indicator,
member_no_email_indicator,
member_dn_mail_indicator,
member_dnaf_email_indicator,
member_dn_email_indicator,
member_refused_give_email_indicator,
integrate_update_batch_number,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_insert_datetime,
effective_end_date,
effective_start_date,
active_indicator,
HASH_VALUE
)
SELECT
STG.customer_adw_key,
STG.member_adw_key,
GENERATE_UUID() AS member_adw_key,
STG.sales_agent_adw_key,
STG.membership_solicitation_adw_key,
STG.member_status_code,
SAFE_CAST(STG.member_check_digit_number AS INT64),
STG.member_associate_identifier,
STG.member_identifier,
SAFE_CAST(STG.member_source_key AS INT64),
SAFE_CAST(STG.member_customer_identifier AS INT64),
STG.member_salutation_name,
STG.member_first_name,
STG.member_middle_name,
STG.member_last_name,
STG.member_suffix_name,
STG.member_gender_code,
DATE(SAFE_CAST(STG.member_birth_date AS DATETIME)),
STG.member_email_name,
STG.member_renew_method_code,
STG.member_commission_code,
STG.member_source_of_sale_code,
STG.member_solicitation_code,
STG.member_associate_relation_code,
DATE(SAFE_CAST(STG.member_join_club_date AS DATETIME)),
DATE(SAFE_CAST(STG.member_join_aaa_date AS DATETIME)),
DATE(SAFE_CAST(STG.member_cancel_date AS DATETIME)),
SAFE_CAST(STG.member_status_datetime AS DATETIME),
STG.member_billing_category_code,
STG.member_billing_code,
STG.member_type_code,
DATE(SAFE_CAST(STG.member_expiration_date AS DATETIME)),
DATE(SAFE_CAST(STG.member_card_expiration_date AS DATETIME)),
STG.member_reason_joined_code,
STG.member_do_not_renew_indicator,
DATE(SAFE_CAST(STG.member_future_cancel_date AS DATETIME)),
STG.member_future_cancel_indicator,
STG.member_free_member_indicator,
STG.member_previous_membership_identifier,
STG.member_previous_club_code,
SAFE_CAST(STG.member_merged_member_key AS INT64),
STG.member_merged_club_code,
STG.member_merged_membership_identifier,
STG.member_merged_associate_identifier,
STG.member_merged_check_digit_number,
STG.member_merged_member16_identifier,
SAFE_CAST(STG.member_name_change_datetime AS DATETIME) ,
SAFE_CAST(STG.member_adm_email_changed_datetime AS DATETIME) ,
STG.member_bad_email_indicator,
STG.member_email_optout_indicator,
SAFE_CAST(STG.member_web_last_login_datetime AS DATETIME),
DATE(SAFE_CAST(STG.member_active_expiration_date AS DATETIME)),
STG.member_dn_text_indicator,
STG.member_duplicate_email_indicator,
STG.member_dn_call_indicator,
STG.member_no_email_indicator,
STG.member_dn_mail_indicator,
STG.member_dnaf_email_indicator,
STG.member_dn_email_indicator,
STG.member_refused_give_email_indicator,
1 AS integrate_update_batch_number,
1 AS integrate_insert_batch_number,
CURRENT_DATETIME() AS integrate_update_datetime,
CURRENT_DATETIME() AS integrate_insert_datetime,
PARSE_DATE("%Y-%m-%d",'9999-12-31') AS effective_end_date,
STG.effective_start_date,
'Y' AS active_indicator,
STG.HASH_VALUE
from `{int_u.INTEGRATION_PROJECT}.adw_work.adw_member_stage_hist` STG
WHERE NOT EXISTS (SELECT 1
FROM `{int_u.INTEGRATION_PROJECT}.adw.adw_member_hist` adw_mem
WHERE CONCAT(adw_mem.member_associate_identifier,adw_mem.member_identifier,SAFE_CAST(adw_mem.member_source_key AS STRING))
=CONCAT(STG.member_associate_identifier,STG.member_identifier,STG.member_source_key)
AND adw_mem.effective_end_date ='9999-12-31')
OR
EXISTS 
(SELECT 1
FROM `{int_u.INTEGRATION_PROJECT}.adw.adw_member_hist` adw_mem
WHERE CONCAT(adw_mem.member_associate_identifier,adw_mem.member_identifier,SAFE_CAST(adw_mem.member_source_key AS STRING))
=CONCAT(STG.member_associate_identifier,STG.member_identifier,STG.member_source_key)
AND adw_mem.effective_end_date ='9999-12-31'
AND adw_mem.HASH_VALUE!=STG.HASH_VALUE)

"""

member_update = f"""
UPDATE  `{int_u.INTEGRATION_PROJECT}.adw.adw_member` adw_member
SET adw_member.effective_end_date = TEMP.END_DT,
adw_member.active_indicator = TEMP.ACTV_FL
FROM (
SELECT member_associate_identifier,member_identifier,member_source_key,effective_start_date,effective_end_date,active_indicator,HASH_VALUE ,
CASE WHEN 
LEAD( effective_start_date ) OVER (PARTITION BY member_associate_identifier,member_identifier,member_source_key ORDER BY effective_start_date,HASH_VALUE)  IS NULL
THEN 
'9999-12-31'
ELSE
LEAD(effective_start_date) OVER (PARTITION BY member_associate_identifier,member_identifier,member_source_key ORDER BY effective_start_date,HASH_VALUE) END END_DT,
CASE WHEN 
LEAD(effective_start_date) OVER (PARTITION BY member_associate_identifier,member_identifier,member_source_key ORDER BY effective_start_date,HASH_VALUE)  IS NOT NULL
THEN 
'N'
ELSE
active_indicator END ACTV_FL
FROM 
`{int_u.INTEGRATION_PROJECT}.adw.adw_member_hist` 
ORDER BY 1,2,3 ) TEMP
WHERE adw_member.member_associate_identifier=TEMP.member_associate_identifier
and COALESCE(adw_member.member_identifier,'')=COALESCE(TEMP.member_identifier,'')
and adw_member.member_source_key=TEMP.member_source_key
AND COALESCE(SAFE_CAST(adw_member.effective_start_date AS STRING),'')=COALESCE(SAFE_CAST(TEMP.effective_start_date AS STRING),'')
AND adw_member.HASH_VALUE=TEMP.HASH_VALUE


"""


####################################################################################################################################
# Member_rider
####################################################################################################################################

#################################
# Member_rider - Stage Load
#################################
# Member_rider


member_rider_work_transformed = f"""

create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.member_rider_hist_stage` as
select distinct * from (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY member_rider_source_key,adw_row_hash ORDER BY effective_start_date ) AS DUP_CHECK
FROM(
SELECT 
member_adw_key,
GENERATE_UUID() as member_rider_adw_key
,member_rider_status_datetime
,member_rider_comp_code
,member_rider_status_code
,member_rider_effective_date
,member_rider_cancel_date
,member_rider_cancel_reason_code
,member_rider_billing_category_code
,user_role_adw_key
,member_rider_solicitation_code
,member_rider_cost_effective_date
,member_rider_actual_cancel_datetime
,member_auto_renewal_card_key
,member_rider_source_key
,member_rider_do_not_renew_indicator
,member_rider_dues_cost_amount
,member_rider_dues_adjustment_amount
,member_rider_payment_amount
,member_rider_paid_by_code
,member_rider_future_cancel_date
,member_rider_reinstate_indicator
,member_rider_adm_original_cost_amount
,member_rider_reinstate_reason_code
,member_rider_extend_expiration_amount
,member_rider_definition_key
,member_rider_activation_date
,effective_start_date
,MD5(CONCAT(COALESCE(member_rider_status_datetime,'')
,COALESCE(member_rider_comp_code,'')
,COALESCE(member_rider_status_code,'')
,COALESCE(member_rider_effective_date,'')
,COALESCE(member_rider_cancel_date,'')
,COALESCE(member_rider_cancel_reason_code,'')
,COALESCE(member_rider_billing_category_code,'')
,COALESCE(member_rider_solicitation_code,'')
,COALESCE(member_rider_cost_effective_date,'')
,COALESCE(member_rider_actual_cancel_datetime,'')
,COALESCE(member_auto_renewal_card_key,'')
,COALESCE(member_rider_source_key,'')
,COALESCE(member_rider_do_not_renew_indicator,'')
,COALESCE(member_rider_dues_cost_amount,'')
,COALESCE(member_rider_dues_adjustment_amount,'')
,COALESCE(member_rider_payment_amount,'')
,COALESCE(member_rider_paid_by_code,'')
,COALESCE(member_rider_future_cancel_date,'')
,COALESCE(member_rider_reinstate_indicator,'')
,COALESCE(member_rider_adm_original_cost_amount,'')
,COALESCE(member_rider_reinstate_reason_code,'')
,COALESCE(member_rider_extend_expiration_amount,'')
,COALESCE(member_rider_definition_key,'')
,COALESCE(member_rider_activation_date,'')
)) as adw_row_hash
from (
select DISTINCT
member.member_adw_key as member_adw_key, 
rider.STATUS_DT  as member_rider_status_datetime
,RIDER_COMP_CD as member_rider_comp_code
,rider.STATUS as member_rider_status_code
,effective_dt  as member_rider_effective_date
,rider.CANCEL_DT  as member_rider_cancel_date
,CANCEL_REASON_CD as member_rider_cancel_reason_code
,rider.BILLING_CATEGORY_CD as member_rider_billing_category_code
,NULL AS user_role_adw_key --user_role.user_role_adw_key
,rider.SOLICITATION_CD  as member_rider_solicitation_code
,COST_EFFECTIVE_DT  as member_rider_cost_effective_date
,ACTUAL_CANCEL_DT  as member_rider_actual_cancel_datetime
,AUTORENEWAL_CARD_KY as member_auto_renewal_card_key
,RIDER_KY  as member_rider_source_key
,rider.DO_NOT_RENEW_FL as member_rider_do_not_renew_indicator
,DUES_COST_AT  as member_rider_dues_cost_amount
,DUES_ADJUSTMENT_AT  as member_rider_dues_adjustment_amount
,PAYMENT_AT  as member_rider_payment_amount
,PAID_BY_CD as member_rider_paid_by_code
,rider.FUTURE_CANCEL_DT  as member_rider_future_cancel_date
,REINSTATE_FL as member_rider_reinstate_indicator
,ADM_ORIGINAL_COST_AT  as member_rider_adm_original_cost_amount
,REINSTATE_REASON_CD as member_rider_reinstate_reason_code
,EXTEND_EXPIRATION_AT  as member_rider_extend_expiration_amount
,RIDER_DEFINITION_KY  as member_rider_definition_key
,ACTIVATION_DT  as member_rider_activation_date
,rider.last_upd_dt  as effective_start_date
 FROM `{iu.INGESTION_PROJECT}.mzp.rider_hist` rider 
join 
`{int_u.INTEGRATION_PROJECT}.adw.adw_member` member 
on 
SAFE_CAST(rider.member_ky as INT64)=member.member_source_key)
-- left outer join `{int_u.INTEGRATION_PROJECT}.adw.user_role` user_role on user_role.member_agent_id=rider.agent_id
) 
) where dup_check=1


"""

member_rider_insert = f"""

INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.member_rider`
(member_adw_key	,	
member_rider_adw_key	,	
member_rider_status_datetime	,	
member_rider_comp_code	,	
member_rider_status_code	,	
member_rider_effective_date	,	
member_rider_cancel_date	,	
member_rider_cancel_reason_code	,	
member_rider_billing_category_code	,	
user_role_adw_key	,	
member_rider_solicitation_code	,	
member_rider_cost_effective_date	,	
member_rider_actual_cancel_datetime	,	
member_auto_renewal_card_key	,	
member_rider_source_key	,	
member_rider_do_not_renew_indicator	,	
member_rider_dues_cost_amount	,	
member_rider_dues_adjustment_amount	,	
member_rider_payment_amount	,	
member_rider_paid_by_code	,	
member_rider_future_cancel_date	,	
member_rider_reinstate_indicator	,	
member_rider_adm_original_cost_amount	,	
member_rider_reinstate_reason_code	,	
member_rider_extend_expiration_amount	,	
member_rider_definition_key	,	
member_rider_activation_date	,	
integrate_update_batch_number	,	
integrate_insert_batch_number	,	
integrate_update_datetime	,	
integrate_insert_datetime	,	
effective_end_date	,	
effective_start_date	,	
active_indicator	,	
adw_row_hash			
)
SELECT 
member_adw_key,
member_rider_adw_key,
SAFE_CAST(member_rider_status_datetime as DATETIME)
,member_rider_comp_code
,member_rider_status_code
,DATE(SAFE_CAST(member_rider_effective_date as DATETIME)) 
,DATE(SAFE_CAST(member_rider_cancel_date as DATETIME))  
,member_rider_cancel_reason_code
,member_rider_billing_category_code,
user_role_adw_key
,member_rider_solicitation_code  
,DATE(SAFE_CAST(member_rider_cost_effective_date as DATETIME))  
,SAFE_CAST(member_rider_actual_cancel_datetime as DATETIME)
,member_auto_renewal_card_key 
,SAFE_CAST(member_rider_source_key as INT64)
,member_rider_do_not_renew_indicator  
,SAFE_CAST(member_rider_dues_cost_amount as NUMERIC)  
,SAFE_CAST(member_rider_dues_adjustment_amount as NUMERIC)  
,SAFE_CAST(member_rider_payment_amount as NUMERIC)  
,member_rider_paid_by_code 
,DATE(SAFE_CAST(member_rider_future_cancel_date as DATETIME))  
,member_rider_reinstate_indicator
,SAFE_CAST(member_rider_adm_original_cost_amount as NUMERIC) 
,member_rider_reinstate_reason_code
,SAFE_CAST(member_rider_extend_expiration_amount as NUMERIC)  
,SAFE_CAST(member_rider_definition_key as INT64)  
,DATE(SAFE_CAST(member_rider_activation_date as DATETIME))  			,
,1 as integrate_update_batch_number
,1 as integrate_insert_batch_number
,CURRENT_DATETIME() AS integrate_update_datetime
,CURRENT_DATETIME() AS integrate_insert_datetime
,PARSE_DATE("%Y-%m-%d",'9999-12-31') AS effective_end_date
,DATE(SAFE_CAST(STG.effective_start_date AS DATETIME)) AS effective_start_date
,'Y' AS active_indicator
,STG.adw_row_hash 
from `{int_u.INTEGRATION_PROJECT}.adw_work.member_rider_hist_stage` STG
WHERE NOT EXISTS (SELECT 1
FROM `{int_u.INTEGRATION_PROJECT}.member.member_rider` member_rider
WHERE MD5(CAST(member_rider.member_rider_source_key as STRING))=MD5(SAFE_CAST(STG.member_rider_source_key as STRING))
AND member_rider.effective_end_date ='9999-12-31')
OR
EXISTS 
(SELECT 1
FROM `{int_u.INTEGRATION_PROJECT}.member.member_rider` member_rider
WHERE MD5(CAST(member_rider.member_rider_source_key as STRING))=MD5(SAFE_CAST(STG.member_rider_source_key as STRING))
AND member_rider.effective_end_date ='9999-12-31'
AND member_rider.adw_row_hash!=STG.adw_row_hash)


"""

member_rider_update = f"""


UPDATE  `{int_u.INTEGRATION_PROJECT}.member.member_rider` member_rider
SET member_rider.effective_end_date = TEMP.END_DT,
member_rider.active_indicator = TEMP.ACTV_FL
FROM (
SELECT member_adw_key,member_rider_source_key,effective_start_date,effective_end_date , active_indicator,adw_row_hash,
CASE WHEN 
LEAD( effective_start_date ) OVER (PARTITION BY member_adw_key,member_rider_source_key ORDER BY effective_start_date,adw_row_hash)  IS NULL
THEN 
'9999-12-31'
ELSE
DATE_SUB((LEAD(effective_start_date ) OVER (PARTITION BY member_adw_key,member_rider_source_key ORDER BY effective_start_date,adw_row_hash)),INTERVAL 1 DAY) END END_DT,
CASE WHEN 
LEAD(effective_start_date) OVER (PARTITION BY member_adw_key,member_rider_source_key ORDER BY effective_start_date,adw_row_hash)  IS NOT NULL
THEN 
'N'
ELSE
active_indicator END ACTV_FL
FROM 
`{int_u.INTEGRATION_PROJECT}.member.member_rider`  ) TEMP
WHERE  member_rider.member_adw_key=TEMP.member_adw_key
and member_rider.member_rider_source_key=TEMP.member_rider_source_key
AND member_rider.effective_start_date=TEMP.effective_start_date
and member_rider.adw_row_hash=TEMP.adw_row_hash

"""

####################################################################################################################################
# Product
####################################################################################################################################

product_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source`  as 
SELECT 
  code_type
  , code
  , code_desc
  , last_upd_dt
  , ROW_NUMBER() OVER(PARTITION BY source.code_ky ORDER BY source.last_upd_dt DESC) AS dupe_check
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` as source 
WHERE code_type in  ( 'FEETYP', 'COMMCD')  and 
  CAST(source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_product`)


"""
product_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` as 
SELECT 
  CASE code_type
    WHEN 'FEETYP' THEN (SELECT product_category_adw_key  from  `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` where product_category_code  = 'MBR_FEES')  
    WHEN 'COMMCD' THEN (SELECT product_category_adw_key  from  `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` where product_category_code  = 'RCC' )
  END AS product_category_adw_key  
  , concat(ifnull(code_type,''),'|', ifnull(code,'') ) as product_sku 
  , code as product_sku_key
  ,'-1' as vendor_adw_key
  , code_desc as product_sku_description
  , SAFE_CAST(last_upd_dt AS DATETIME)  as effective_start_datetime
  , CAST('9999-12-31' AS datetime) effective_end_datetime
  , 'Y' as active_indicator
  , CURRENT_DATETIME() as integrate_insert_datetime
  , 1 as integrate_insert_batch_number
  , CURRENT_DATETIME() as integrate_update_datetime
  , 1 as integrate_update_batch_number
  , TO_BASE64(MD5(CONCAT(COALESCE(code_type,''),COALESCE(code_desc,'')))) as adw_row_hash 
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source` as source 
WHERE source.dupe_check=1 
"""

product_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source_stage` AS
SELECT
  GENERATE_UUID() as product_adw_key, 
  source.product_category_adw_key,
  source.product_sku,
  source.product_sku_key,
  source.vendor_adw_key,
  source.product_sku_description,
  source.adw_row_hash,
  source.effective_start_datetime,
  source.effective_end_datetime,
  source.active_indicator,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku
    AND target.active_indicator='Y')
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
  target.product_sku_description,
  target.adw_row_hash,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 day) AS effective_end_datetime,
  'N' AS active_indicator,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""

product_merge=f"""
MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.adw.dim_product` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source_stage` b
ON
  (a.product_adw_key = b.product_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  product_adw_key ,    
  product_category_adw_key,
  product_sku,
  product_sku_key,  
  vendor_adw_key,  
  product_sku_description,
  effective_start_datetime,
  effective_end_datetime,
  active_indicator,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number,
  adw_row_hash  
  ) VALUES (
  b.product_adw_key ,    
  b.product_category_adw_key,
  b.product_sku,
  b.product_sku_key,
  b.vendor_adw_key,    
  b.product_sku_description,
  b.effective_start_datetime,
  b.effective_end_datetime,
  b.active_indicator,
  b.integrate_insert_datetime,
  b.integrate_insert_batch_number,
  b.integrate_update_datetime,
  b.integrate_update_batch_number,
  b.adw_row_hash)
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number
"""


#################################################################
# Payment summary
##################################################################

payment_applied_summary_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_source`  as 
SELECT 
membership_payment_ky
,membership_ky
,payment_dt
,payment_at
,batch_name
,payment_method_cd
,cc_type_cd
,cc_expiration_dt
,transaction_type_cd
,adjustment_description_cd
,source_cd
,create_dt
,paid_by_cd
,cc_authorization_nr
,reason_cd
,check_nr
,check_dt
,rev_membership_payment_ky
,donor_nr
,bill_summary_ky
,pnref
,user_id
,unapplied_at
,parent_payment_ky
,pos_sale_id
,pos_ofc_id
,pos_cust_recpt_nr
,batch_ky
,pos_comment
,pos_consultant_id
,cc_first_name
,cc_middle_name
,cc_last_name
,cc_street
,cc_zip
,ach_bank_name
,ach_bank_account_type
,ach_bank_account_name
,ach_authorization_nr
,ach_street
,ach_city
,ach_state
,ach_zip
,ach_email
,transaction_id
,cc_transaction_id
,advance_pay_at
,ach_token
,transaction_cd
,branch_ky
,cc_last_four
,cc_state
,cc_city
,last_upd_dt
,adw_lake_insert_datetime
,adw_lake_insert_batch_number 
,ROW_NUMBER() OVER(PARTITION BY membership_payment_ky ORDER BY last_upd_dt DESC) AS dupe_check
FROM `{iu.INGESTION_PROJECT}.mzp.payment_summary` 
WHERE CAST(last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary`)

"""

payment_applied_summary_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` as 
SELECT 
 SAFE_CAST(membership_payment_ky as int64)  as payment_applied_source_key 
,COALESCE(aca_office.aca_office_adw_key,'-1') as aca_office_adw_key
,COALESCE(membership.membership_adw_key,'-1') as membership_adw_key
,SAFE_CAST (SAFE_CAST(payment_dt as datetime) as date) as payment_applied_date 
,SAFE_CAST(payment_at as numeric)  as payment_applied_amount
,batch_name as payment_batch_name
,payment_method_cd as payment_method_code
,cx_codes1.code_desc as payment_method_description
,transaction_type_cd as payment_type_code
,cx_codes2.code_desc as payment_type_description
,adjustment_description_cd as payment_adjustment_code
,cx_codes3.code_desc as payment_adjustment_description
,SAFE_CAST(create_dt as datetime)  as payment_created_datetime
,CASE paid_by_cd
    WHEN 'P' THEN 'Primary'
    WHEN 'D' THEN 'Donor'
    WHEN 'A' THEN 'Associate'
END AS payment_paid_by_code  
,COALESCE(rev_membership_payment_ky,'')  as membership_payment_applied_reversal_adw_key
,COALESCE(employee.employee_adw_key,'-1')  as employee_adw_key
,SAFE_CAST(unapplied_at as numeric)  as payment_unapplied_amount
,COALESCE(payments_received.membership_payments_received_adw_key,'-1') as membership_payments_received_adw_key
,TO_BASE64(MD5(CONCAT(COALESCE(payment_dt,'')
,COALESCE( payment_at,'')
,COALESCE( payment_dt,'')
,COALESCE( batch_name,'')
,COALESCE( payment_method_cd,'')
,COALESCE( transaction_type_cd,'')
,COALESCE( adjustment_description_cd,'')
,COALESCE( create_dt,'')
,COALESCE( rev_membership_payment_ky,'')
,COALESCE( cx_codes1.code_desc,'')
,COALESCE( cx_codes2.code_desc,'')
,COALESCE( cx_codes3.code_desc,'')
,COALESCE( unapplied_at,'')
,COALESCE( membership_payment_ky,'-1')
,COALESCE( membership.membership_adw_key,'-1')
,COALESCE( rev_membership_payment_ky,'-1')
,COALESCE( employee.employee_adw_key,'-1')
,COALESCE( payments_received.membership_payments_received_adw_key,'-1')
))) as adw_row_hash
,SAFE_CAST(source.last_upd_dt AS DATETIME)  as effective_start_datetime
,CAST('9999-12-31' AS datetime) effective_end_datetime
,'Y' as active_indicator
,CURRENT_DATETIME() as integrate_insert_datetime
,1 as integrate_insert_batch_number
,CURRENT_DATETIME() as integrate_update_datetime
,1 as integrate_update_batch_number
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_source` as source 
LEFT JOIN
  `{iu.INGESTION_PROJECT}.mzp.cx_codes` cx_codes1 ON (
  source.payment_method_cd=cx_codes1.code and cx_codes1.code_type='PEMTH' )
LEFT JOIN
  `{iu.INGESTION_PROJECT}.mzp.cx_codes` cx_codes2 ON (
  source.payment_method_cd=cx_codes2.code and cx_codes2.code_type='PAYSRC' )
LEFT JOIN
  `{iu.INGESTION_PROJECT}.mzp.cx_codes` cx_codes3 ON (
  source.payment_method_cd=cx_codes3.code and cx_codes3.code_type='PAYADJ' )
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership ON (
   source.membership_ky=SAFE_CAST(membership.membership_source_system_key as STRING) and membership.active_indicator='Y' )
LEFT JOIN (select max(membership_payments_received_adw_key) as membership_payments_received_adw_key,payment_received_batch_key,active_indicator
          from `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` 
          group by payment_received_batch_key,active_indicator ) payments_received 
          ON source.batch_ky=SAFE_CAST(payments_received.payment_received_batch_key as STRING) 
          and payments_received.active_indicator='Y' 
LEFT JOIN (select branch_cd,branch_ky, 
          ROW_NUMBER() OVER(PARTITION BY branch_cd ORDER BY NULL DESC) AS dupe_check
          from `{iu.INGESTION_PROJECT}.mzp.branch` ) branch 
          on source.branch_ky = branch.branch_ky  and branch.dupe_check = 1
LEFT JOIN (select max(aca_office_adw_key) as aca_office_adw_key,membership_branch_code,active_indicator
          from `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` 
          group by membership_branch_code,active_indicator ) aca_office 
          ON branch.branch_cd = aca_office.membership_branch_code and aca_office.active_indicator='Y' 
LEFT JOIN (select user_id,username,
          ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
          from `{iu.INGESTION_PROJECT}.mzp.cx_iusers` ) cx_iusers
          ON source.user_id = cx_iusers.user_id  and cx_iusers.dupe_check = 1
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` employee ON (
cx_iusers.username=SAFE_CAST(employee.employee_active_directory_user_identifier as STRING) and employee.active_indicator ='Y' )
WHERE source.dupe_check=1 
"""

payment_summary_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_source_stage` AS
SELECT
 GENERATE_UUID() as membership_payment_applied_summary_adw_key 
,source.membership_payment_applied_reversal_adw_key
,source.payment_applied_source_key
,source.membership_adw_key
,source.membership_payments_received_adw_key
,source.aca_office_adw_key
,source.employee_adw_key
,source.payment_applied_date
,source.payment_applied_amount
,source.payment_batch_name
,source.payment_method_code
,source.payment_method_description
,source.payment_type_code
,source.payment_type_description
,source.payment_adjustment_code
,source.payment_adjustment_description
,source.payment_created_datetime
,source.payment_paid_by_code
,source.payment_unapplied_amount
,source.effective_start_datetime
,source.effective_end_datetime
,source.active_indicator
,source.adw_row_hash
,source.integrate_insert_datetime
,source.integrate_insert_batch_number
,source.integrate_update_datetime
,source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` target
ON
  (source.payment_applied_source_key=target.payment_applied_source_key
    AND target.active_indicator='Y')
WHERE
  target.membership_payment_applied_summary_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
 membership_payment_applied_summary_adw_key 
,target.membership_payment_applied_reversal_adw_key
,target.payment_applied_source_key
,target.membership_adw_key
,target.membership_payments_received_adw_key
,target.aca_office_adw_key
,target.employee_adw_key
,target.payment_applied_date
,target.payment_applied_amount
,target.payment_batch_name
,target.payment_method_code
,target.payment_method_description
,target.payment_type_code
,target.payment_type_description
,target.payment_adjustment_code
,target.payment_adjustment_description
,target.payment_created_datetime
,target.payment_paid_by_code
,target.payment_unapplied_amount
,target.effective_start_datetime
,DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 day) AS effective_end_datetime
,'N' AS active_indicator
,target.adw_row_hash
,target.integrate_insert_datetime
,target.integrate_insert_batch_number
,CURRENT_DATETIME()
,1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` target
ON
  (source.payment_applied_source_key=target.payment_applied_source_key
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""
payment_summary_merge = f"""
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_source_stage` b
  ON
    (a.membership_payment_applied_summary_adw_key = b.membership_payment_applied_summary_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( 
   membership_payment_applied_summary_adw_key 
  ,membership_payment_applied_reversal_adw_key
  ,payment_applied_source_key
  ,membership_adw_key
  ,membership_payments_received_adw_key
  ,aca_office_adw_key
  ,employee_adw_key
  ,payment_applied_date
  ,payment_applied_amount
  ,payment_batch_name
  ,payment_method_code
  ,payment_method_description
  ,payment_type_code
  ,payment_type_description
  ,payment_adjustment_code
  ,payment_adjustment_description
  ,payment_created_datetime
  ,payment_paid_by_code
  ,payment_unapplied_amount
  ,effective_start_datetime
  ,effective_end_datetime
  ,active_indicator
  ,adw_row_hash
  ,integrate_insert_datetime
  ,integrate_insert_batch_number
  ,integrate_update_datetime
  ,integrate_update_batch_number

   ) VALUES (
  b.membership_payment_applied_summary_adw_key 
  ,b.membership_payment_applied_reversal_adw_key
  ,b.payment_applied_source_key
  ,b.membership_adw_key
  ,b.membership_payments_received_adw_key
  ,b.aca_office_adw_key
  ,b.employee_adw_key
  ,b.payment_applied_date
  ,b.payment_applied_amount
  ,b.payment_batch_name
  ,b.payment_method_code
  ,b.payment_method_description
  ,b.payment_type_code
  ,b.payment_type_description
  ,b.payment_adjustment_code
  ,b.payment_adjustment_description
  ,b.payment_created_datetime
  ,b.payment_paid_by_code
  ,b.payment_unapplied_amount
  ,b.effective_start_datetime
  ,b.effective_end_datetime
  ,b.active_indicator
  ,b.adw_row_hash
  ,b.integrate_insert_datetime
  ,b.integrate_insert_batch_number
  ,b.integrate_update_datetime
  ,b.integrate_update_batch_number
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
"""


####################################################################################################################################
# Membership segments
####################################################################################################################################

membership_segment_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_source`  as 
SELECT 
segmentation_history_ky
,membership_ky
,historysource.segmentation_setup_ky
,membership_exp_dt
,historysource.segmentation_test_end_dt
,historysource.segmentation_schedule_dt
,panel_cd
,segmentation_panel_cd
,control_panel_fl
,commission_cd
,setupsource.name as name
,historysource.adw_lake_insert_datetime
,historysource.adw_lake_insert_batch_number
,historysource.adw_lake_insert_datetime as last_upd_dt
,ROW_NUMBER() OVER(PARTITION BY historysource.segmentation_history_ky ORDER BY historysource.membership_exp_dt DESC) AS dupe_check
FROM `{iu.INGESTION_PROJECT}.mzp.segmentation_history` as historysource 
JOIN (select 
      *,
      row_number() over (partition by name order by segmentation_test_end_dt desc) as rn	  
      from `{iu.INGESTION_PROJECT}.mzp.segmentation_setup` ) setupsource on (historysource.segmentation_setup_ky=setupsource.segmentation_setup_ky and setupsource.rn=1)
WHERE CAST(historysource.adw_lake_insert_datetime AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation`)

"""

membership_segment_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` as 
SELECT 
  segmentation_history_ky
 ,COALESCE(membership.membership_adw_key,'-1') as membership_adw_key 
 ,SAFE_CAST(SAFE_CAST(membership_exp_dt AS datetime) as date) as member_expiration_dt 
 ,panel_cd as segmentation_test_group_indicator
 ,segmentation_panel_cd as segmentation_panel_code
 ,control_panel_fl as  segmentation_control_panel_indicator
 ,commission_cd as segmentation_commission_code
 ,name as segment_name
 ,  concat(ifnull(membership_ky,''),'|', SAFE_CAST(SAFE_CAST(source.membership_exp_dt AS date) AS STRING), '|', ifnull(name,'')) as segment_key
 ,  SAFE_CAST(last_upd_dt as datetime) as effective_start_datetime
  , CAST('9999-12-31' AS datetime) effective_end_datetime
  , 'Y' as active_indicator
  , CURRENT_DATETIME() as integrate_insert_datetime
  , 1 as integrate_insert_batch_number
  , CURRENT_DATETIME() as integrate_update_datetime
  , 1 as integrate_update_batch_number
  , TO_BASE64(MD5(CONCAT(COALESCE(panel_cd,''),
                         COALESCE(segmentation_panel_cd,''),
                         COALESCE(control_panel_fl,''),
                         COALESCE(membership.membership_adw_key,''),
                         COALESCE(commission_cd,'')))) as adw_row_hash 
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_source` as source 
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership
ON
  (source.membership_ky=SAFE_CAST(membership.membership_source_system_key as STRING) AND membership.active_indicator='Y')
WHERE source.dupe_check=1 

"""
membership_segment_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_stage` AS
SELECT
  COALESCE(target.membership_marketing_segmentation_adw_key, GENERATE_UUID()) membership_marketing_segmentation_adw_key
  ,source.membership_adw_key
  ,source.member_expiration_dt
  ,source.segmentation_test_group_indicator
  ,source.segmentation_panel_code
  ,source.segmentation_control_panel_indicator
  ,source.segmentation_commission_code
  ,source.segment_name
  ,source.adw_row_hash
  ,source.effective_start_datetime
  ,source.effective_end_datetime
  ,source.active_indicator
  ,source.integrate_insert_datetime
  ,source.integrate_insert_batch_number
  ,source.integrate_update_datetime
  ,source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation` target
ON
  (source.segment_key=concat(ifnull(target.membership_adw_key,''),'|', SAFE_CAST(SAFE_CAST(target.member_expiration_date AS date) AS STRING), '|', ifnull(target.segment_name,'')) 
    AND target.active_indicator='Y')
WHERE
  target.membership_marketing_segmentation_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
  target.membership_marketing_segmentation_adw_key
  ,target.membership_adw_key
  ,target.member_expiration_date
  ,target.segmentation_test_group_indicator
  ,target.segmentation_panel_code
  ,target.segmentation_control_panel_indicator
  ,target.segmentation_commission_code
  ,target.segment_name
  ,target.adw_row_hash
  ,target.effective_start_datetime
  ,DATETIME_SUB(SAFE_CAST(source.effective_start_datetime as datetime),
    INTERVAL 1 day) AS effective_end_datetime
  ,'N' AS active_indicator
  ,target.integrate_insert_datetime
  ,target.integrate_insert_batch_number
  ,CURRENT_DATETIME()
  ,1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation` target
ON
  (source.segment_key=concat(ifnull(target.membership_adw_key,''),'|', SAFE_CAST(SAFE_CAST(target.member_expiration_date AS date) AS STRING), '|', ifnull(target.segment_name,'')) 
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash
"""

membership_segment_merge = f"""
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_stage` b
  ON
    (a.membership_marketing_segmentation_adw_key = b.membership_marketing_segmentation_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( 
    membership_marketing_segmentation_adw_key 
    ,membership_adw_key
    ,member_expiration_date
    ,segmentation_test_group_indicator
    ,segmentation_panel_code
    ,segmentation_control_panel_indicator
    ,segmentation_commission_code
    ,segment_name
    ,adw_row_hash
    ,effective_start_datetime
    ,effective_end_datetime
    ,active_indicator
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
   ) VALUES (
     b.membership_marketing_segmentation_adw_key
    ,b.membership_adw_key
    ,b.member_expiration_dt
    ,b.segmentation_test_group_indicator
    ,b.segmentation_panel_code
    ,b.segmentation_control_panel_indicator
    ,b.segmentation_commission_code
    ,b.segment_name
    ,b.adw_row_hash
    ,b.effective_start_datetime
    ,b.effective_end_datetime
    ,b.active_indicator
    ,b.integrate_insert_datetime
    ,b.integrate_insert_batch_number
    ,b.integrate_update_datetime
    ,b.integrate_update_batch_number
  )
"""


with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    #Summary
    task_payment_applied_summary_source = int_u.run_query("payment_applied_summary_source", payment_applied_summary_source)
    task_payment_applied_summary_transformed = int_u.run_query("payment_applied_summary_transformed", payment_applied_summary_transformed)
    task_payment_summary_stage = int_u.run_query("payment_summary_stage",payment_summary_stage)
    task_payment_summary_merge = int_u.run_query('payment_summary_merge', payment_summary_merge)



    # Membership - Workarea loads
    #task_membership_work_source = int_u.run_query("build_membership_work_source", membership_work_source)
    #task_membership_work_transformed = int_u.run_query("build_membership_work_transformed", membership_work_transformed)
    #task_membership_work_final_staging = int_u.run_query("build_membership_work_final_staging",membership_work_final_staging)

    #task_membership_merge = int_u.run_query('merge_membership', membership_merge)

    
    # Membership

    #task_membership_work_source >> task_membership_work_transformed
    #task_membership_work_transformed >> task_membership_work_final_staging
    #task_membership_work_final_staging >> task_membership_merge

    # Product
    #task_product_source = int_u.run_query('product_source', product_source)
    #task_product_transformed = int_u.run_query('product_transformed', product_transformed)
    #task_product_stage = int_u.run_query('product_stage', product_stage)
    #task_product_merge = int_u.run_query('product_merge', product_merge) 
    
    #Membership
    



    #task_membership_segment_source = int_u.run_query('membership_segment_source', membership_segment_source)
    #task_membership_segment_transformed = int_u.run_query('membership_segment_transformed', membership_segment_transformed)
    #task_membership_segment_stage = int_u.run_query('membership_segment_stage', membership_segment_stage)
    #task_membership_segment_merge = int_u.run_query('membership_segment_merge', membership_segment_merge)

    ######################################################################
    # DEPENDENCIES

    # Membership segment
    #task_membership_segment_source >> task_membership_segment_transformed
    #task_membership_segment_transformed >> task_membership_segment_stage
    #task_membership_segment_stage >> task_membership_segment_merge
    
        
    # Product
    #task_product_source >> task_product_transformed
    #task_product_transformed >> task_product_stage
    #task_product_stage >> task_product_merge
    
    #Summary
    
    task_payment_applied_summary_source >> task_payment_applied_summary_transformed
    task_payment_applied_summary_transformed >> task_payment_summary_stage
    task_payment_summary_stage >> task_payment_summary_merge
