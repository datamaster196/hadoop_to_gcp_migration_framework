from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_membership_mzp_hist"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22),
    'email': ['airflow@example.com'],
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# membership_gl_payments_applied SQL START
######################################################################
# Load membership_gl_payments_applied Historical set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

membership_gl_payments_applied_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
   `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_source` AS
 SELECT
 membership_gl_pay_app_source.JOURNAL_ENTRY_KY ,
 membership_gl_pay_app_source.GL_ACCOUNT_KY ,
 membership_gl_pay_app_source.membership_ky,
 membership_gl_pay_app_source.POST_DT ,
 membership_gl_pay_app_source.PROCESS_DT ,
 membership_gl_pay_app_source.Journal_at ,
 membership_gl_pay_app_source.CREATE_DT ,
 membership_gl_pay_app_source.DR_CR_IND ,
 membership_gl_pay_app_source.last_upd_dt, 

   ROW_NUMBER() OVER(PARTITION BY  membership_gl_pay_app_source.JOURNAL_ENTRY_KY, membership_gl_pay_app_source.last_upd_dt  ORDER BY  null ) AS dupe_check
 FROM
   `{iu.INGESTION_PROJECT}.mzp.journal_entry` AS membership_gl_pay_app_source

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_gl_payments_applied_work_transformed = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` AS
SELECT
  COALESCE(membership.membership_adw_key,
    "-1") AS membership_adw_key,
  COALESCE(office.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  CAST(membership_gl_pay_app_source.JOURNAL_ENTRY_KY AS INT64) AS gl_payment_journal_source_key,
  gl_account.GL_DESCR AS gl_payment_gl_description,
  gl_account.GL_ACCT_NUMBER AS gl_payment_account_number,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.POST_DT,1,10)) AS Date) AS gl_payment_post_date,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.PROCESS_DT,1,10)) AS Date) AS gl_payment_process_date,
  SAFE_CAST(membership_gl_pay_app_source.Journal_at AS NUMERIC) AS gl_payment_journal_amount,
  SAFE_CAST(membership_gl_pay_app_source.CREATE_DT AS DATETIME) AS gl_payment_journal_created_datetime,
  membership_gl_pay_app_source.DR_CR_IND AS gl_payment_journal_debit_credit_indicator,
  membership_gl_pay_app_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(membership.membership_adw_key,
            "-1"),
          ''),'|', ifnull(COALESCE(office.aca_office_adw_key,
            "-1"),
          ''),'|', ifnull(gl_account.GL_DESCR,
          ''),'|', ifnull(gl_account.GL_ACCT_NUMBER,
          ''),'|',ifnull(membership_gl_pay_app_source.POST_DT,
          ''),'|',ifnull(membership_gl_pay_app_source.PROCESS_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.Journal_at,
          ''), '|',ifnull(membership_gl_pay_app_source.CREATE_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.DR_CR_IND,
          '')))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_source` AS membership_gl_pay_app_source
LEFT OUTER JOIN (
  SELECT
    membership_adw_key,
    membership_source_system_key,
    active_indicator
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership`
  WHERE
    active_indicator='Y' ) membership
ON
  CAST(membership.membership_source_system_key AS string)=membership_gl_pay_app_source.membership_ky
LEFT OUTER JOIN (
  SELECT
    GL_ACCOUNT_KY,
    GL_DESCR,
    GL_ACCT_NUMBER,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY GL_ACCOUNT_KY ORDER BY NULL ) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.gl_account` ) gl_account
ON
  gl_account.GL_ACCOUNT_KY=membership_gl_pay_app_source.GL_ACCOUNT_KY
  AND gl_account.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    BRANCH_CD,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_KY ORDER BY NULL) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.branch` ) branch
ON
  branch.BRANCH_KY=gl_account.BRANCH_KY
  AND branch.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    membership_branch_code
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office`
  WHERE
    active_indicator='Y') office
ON
  office.membership_branch_code=branch.BRANCH_KY
WHERE
  membership_gl_pay_app_source.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_gl_payments_applied_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_type_2_hist` AS
WITH
  membership_gl_payments_applied_hist AS (
  SELECT
    gl_payment_journal_source_key,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY gl_payment_journal_source_key ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(cast(effective_start_datetime as datetime)) OVER (PARTITION BY gl_payment_journal_source_key ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    gl_payment_journal_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    membership_gl_payments_applied_hist 
  ), set_groups AS (
  SELECT
    gl_payment_journal_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY gl_payment_journal_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    gl_payment_journal_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    gl_payment_journal_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  gl_payment_journal_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_gl_payments_applied
membership_gl_payments_applied_insert = f"""
INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied` ( membership_gl_payments_applied_adw_key,
    membership_adw_key,
    aca_office_adw_key,
    gl_payment_journal_source_key,
    gl_payment_gl_description,
    gl_payment_account_number,
    gl_payment_post_date,
    gl_payment_process_date,
    gl_payment_journal_amount,
    gl_payment_journal_created_datetime,
    gl_payment_journal_debit_credit_indicator,
    effective_start_datetime,
    effective_end_datetime,
    active_indicator,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  membership_gl_payments_applied_adw_key,
  source. membership_adw_key,
  source. aca_office_adw_key,
  source. gl_payment_journal_source_key,
  source. gl_payment_gl_description,
  source. gl_payment_account_number,
  source. gl_payment_post_date,
  source. gl_payment_process_date,
  source. gl_payment_journal_amount,
  source. gl_payment_journal_created_datetime,
  source. gl_payment_journal_debit_credit_indicator,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` source
ON
  ( source.gl_payment_journal_source_key=hist_type_2.gl_payment_journal_source_key
    AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_gl_payments_applied_adw_key,
    gl_payment_journal_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed`
  GROUP BY
    gl_payment_journal_source_key) pk
ON
  source.gl_payment_journal_source_key=pk.gl_payment_journal_source_key

"""
# membership_gl_payments_applied SQL END
# membership_gl_payments_applied(SAHIL)

######################################################################
# Load Product Historical set of Data
######################################################################

product_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source`  as 
SELECT 
  code_type
  , code
  , code_desc
  , last_upd_dt
  , ROW_NUMBER() OVER(PARTITION BY source.code_ky, source.last_upd_dt ORDER BY  NULL DESC) AS dupe_check 
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` as source 
WHERE code_type in  ( 'FEETYP', 'COMPCD') 
"""

product_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` as 
SELECT 
  CASE code_type
    WHEN 'FEETYP' THEN (SELECT product_category_adw_key  from  `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` where product_category_code  = 'MBR_FEES')  
    WHEN 'COMPCD' THEN (SELECT product_category_adw_key  from  `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` where product_category_code  = 'RCC' )
  END AS product_category_adw_key  
  , concat(ifnull(code_type,''),'|', ifnull(code,'') ) as product_sku 
  , code as product_sku_key
  , '-1' as vendor_adw_key
  , code_desc as product_sku_description
  , SAFE_CAST(last_upd_dt AS DATETIME)  as last_upd_dt
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

product_work_type2_logic = f"""

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_product_work_type_2_hist` AS
WITH
  product_hist AS (
  SELECT
    product_sku,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY product_sku ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY product_sku ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` 
  ), set_grouping_column AS (
  SELECT
    product_sku,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    product_hist 
  ), set_groups AS (
  SELECT
    product_sku,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY product_sku ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    product_sku,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    product_sku,
    adw_row_hash,
    grouping_column 
  )
SELECT
  product_sku,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""
product_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.adw.dim_product` 
  ( product_adw_key ,    
  product_category_adw_key,
  product_sku,
  product_sku_key,    
  vendor_adw_key,
  product_sku_description,
  adw_row_hash,
  effective_start_datetime,
  effective_end_datetime,
  active_indicator,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number
  )
SELECT
  product_adw_key ,    
  source.product_category_adw_key,
  source.product_sku,
  source.product_sku_key,   
  source.vendor_adw_key, 
  source.product_sku_description,
  source.adw_row_hash,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_product_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` source ON (
  source.product_sku=hist_type_2.product_sku
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS product_adw_key,
    product_sku
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed`
  GROUP BY
    product_sku) pk
ON (source.product_sku=pk.product_sku)

"""

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
    ROW_NUMBER() OVER(PARTITION BY membership_source.membership_id, membership_source.last_upd_dt ORDER BY  NULL DESC) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.membership` AS membership_source

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
           --''),'|',ifnull(membership.coverage_level_cd,
            ''),'|',ifnull(membership.group_cd,
           -- ''),'|',ifnull(membership.coverage_level_ky,
            ''),'|',ifnull(membership.ebill_fl,
            ''),'|',ifnull(membership.segmentation_cd,
            ''),'|',ifnull(membership.promo_code,
            ''),'|',ifnull(membership.phone_type,
            ''),'|',ifnull(phone.phone_adw_key,
          '-1'),'|',ifnull(product.product_adw_key,
          '-1'),'|',ifnull(aca_office.aca_office_adw_key,
          '-1'),'|',ifnull(CAST(membership_code.ERS_CODE AS string),
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
    (select product_sku_key,
            product_adw_key,
            ROW_NUMBER() over (PARTITION BY product_sku_key) DUPE_CHECK
      FROM `{int_u.INTEGRATION_PROJECT}.adw.dim_product`
      WHERE active_indicator = 'Y' ) product
  ON
    membership.coverage_level_cd = product.product_sku_key and product.DUPE_CHECK=1
  LEFT JOIN
    `{iu.INGESTION_PROJECT}.mzp.branch` as branch
  ON membership.branch_ky = branch.branch_ky
    left join `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` as aca_office
        on branch.branch_cd = aca_office.membership_branch_code
    left join (select membership_id,
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

membership_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_type_2_hist` AS
WITH
  membership_hist AS (
  SELECT
    membership_identifier,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY membership_identifier ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY membership_identifier ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    membership_identifier,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    membership_hist 
  ), set_groups AS (
  SELECT
    membership_identifier,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY membership_identifier ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    membership_identifier,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    membership_identifier,
    adw_row_hash,
    grouping_column 
  )
SELECT
  membership_identifier,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership
membership_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership` 
  ( membership_adw_key,
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
    integrate_update_batch_number )
SELECT
  membership_adw_key,
  source.address_adw_key,
  CAST(source.phone_adw_key AS string),
  CAST (source.product_adw_key AS string),	  
  CAST(source.aca_office_adw_key AS string),
  CAST(source.membership_source_system_key AS int64),
  source.membership_identifier,
  source.membership_status_code,
  source.membership_billing_code,
  CAST(source.membership_status_datetime AS datetime),
  CAST(substr(source.membership_cancel_date, 0, 10) AS date),
  source.membership_billing_category_code,
  CAST(source.membership_dues_cost_amount AS numeric),
  CAST(source.membership_dues_adjustment_amount AS numeric),
  source.membership_future_cancel_indicator,
  CAST(substr(source.membership_future_cancel_date, 0, 10) AS date),
  source.membership_out_of_territory_code,
  CAST(source.membership_address_change_datetime AS datetime),
  CAST(source.membership_cdx_export_update_datetime AS datetime),
  CAST(source.membership_tier_key AS float64),
  source.membership_temporary_address_indicator,
  source.previous_club_membership_identifier,
  source.previous_club_code,
  source.membership_transfer_club_code,
  CAST(source.membership_transfer_datetime AS datetime),
  CAST(source.merged_membership_previous_club_code AS string),
  CAST(source.merged_member_previous_membership_identifier AS string),
  safe_cast(source.merged_member_previous_membership_key as int64) as merged_member_previous_membership_key,
  source.membership_dn_solicit_indicator,
  source.membership_bad_address_indicator,
  source.membership_dn_send_publications_indicator,
  source.membership_market_tracking_code,
  source.membership_salvage_flag,
  CAST(source.membership_salvaged_indicator AS string),
  CAST(source.membership_dn_salvage_indicator AS string),
  --source.membership_coverage_level_code,
  --CAST(source.membership_coverage_level_key AS int64),
  source.membership_group_code,
  source.membership_type_code,
  source.membership_ebill_indicator,
  source.membership_marketing_segmentation_code,
  source.membership_promo_code,
  source.membership_phone_type_code,
  CAST(source.membership_ers_abuser_indicator AS string),
  CAST(source.membership_dn_send_aaaworld_indicator AS string),
  CAST(source.membership_shadow_motorcycle_indicator AS string),
  CAST(source.membership_student_indicator AS string),
  CAST(source.membership_address_updated_by_ncoa_indicator AS string),
  CAST(source.membership_assigned_retention_indicator AS string),
  CAST(source.membership_no_membership_promotion_mailings_indicator AS string),
  CAST(source.membership_dn_telemarket_indicator AS string),
  CAST(source.membership_dn_offer_plus_indicator AS string),
  CAST(source.membership_receive_aaaworld_online_edition_indicator AS string),
  CAST(source.membership_shadowrv_coverage_indicator AS string),
  CAST(source.membership_dn_direct_mail_solicit_indicator AS string),
  CAST(source.membership_heavy_ers_indicator AS string),
  CAST(source.membership_purge_indicator AS string),
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` source ON (
  source.membership_identifier=hist_type_2.membership_identifier
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_adw_key,
    membership_identifier
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed`
  GROUP BY
    membership_identifier) pk
ON (source.membership_identifier=pk.membership_identifier);

"""
####################################################################################################################################
# Member
####################################################################################################################################
######################################################################
# Load Membership Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


member_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_source` AS
 SELECT
 member_source.MEMBER_KY ,
 member_source.MEMBERSHIP_KY,
member_source.STATUS ,
member_source.CHECK_DIGIT_NR ,
member_source.ASSOCIATE_ID ,
member_source.MEMBERSHIP_ID ,
member_source.MEMBER_TYPE_CD ,
member_source.RENEW_METHOD_CD ,
member_source.COMMISSION_CD ,
member_source.SOURCE_OF_SALE ,
member_source.SOLICITATION_CD ,
member_source.ASSOCIATE_RELATION_CD,
member_source.JOIN_CLUB_DT,
member_source.JOIN_AAA_DT,
member_source.CANCEL_DT,
member_source.STATUS_DT,
member_source.BILLING_CATEGORY_CD,
member_source.BILLING_CD,
'N' as MEMBERSHIP_PURGE_INDICATOR,
member_source.MEMBER_EXPIRATION_DT,
member_source.MEMBER_CARD_EXPIRATION_DT,
member_source.REASON_JOINED,
member_source.DO_NOT_RENEW_FL,
member_source.FUTURE_CANCEL_DT,
member_source.FUTURE_CANCEL_FL,
member_source.FREE_MEMBER_FL,
member_source.PREVIOUS_MEMBERSHIP_ID,
member_source.PREVIOUS_CLUB_CD,
member_source.NAME_CHANGE_DT,
member_source.ADM_EMAIL_CHANGED_DT,
member_source.BAD_EMAIL_FL,
member_source.EMAIL_OPTOUT_FL,
member_source.WEB_LAST_LOGIN_DT,
member_source.ACTIVE_EXPIRATION_DT,
member_source.ACTIVATION_DT,
member_source.AGENT_ID,
member_source.LAST_UPD_DT,
ROW_NUMBER() OVER(PARTITION BY member_source.MEMBER_KY, member_source.LAST_UPD_DT ORDER BY NULL DESC) AS DUPE_CHECK
  FROM
  `{iu.INGESTION_PROJECT}.mzp.member` AS member_source

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

member_work_transformed = f"""
--Replace {int_u.INTEGRATION_PROJECT}. with {int_u.INTEGRATION_PROJECT}.
-- Replace {iu.INGESTION_PROJECT}.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_transformed` AS
 SELECT
 adw_memship.membership_adw_key as membership_adw_key,
 cust_src_key.contact_adw_key as contact_adw_key,
solicitation.membership_solicitation_adw_key,
 role.employee_role_adw_key as employee_role_adw_key ,
 member.member_ky as member_source_system_key ,
 member.membership_id as membership_identifier,
 member.associate_id as member_associate_identifier,
 member.check_digit_nr as member_check_digit_number,
 member.status as member_status_code ,
member.member_expiration_dt as member_expiration_date,
member.member_type_cd as primary_member_indicator,
member.do_not_renew_fl as member_do_not_renew_indicator ,
member.billing_cd as member_billing_code,
member.billing_category_cd as member_billing_category_code,
member.member_card_expiration_dt as member_card_expiration_dt,
member.status_dt as member_status_datetime,
member.cancel_dt as member_cancel_date ,
member.join_aaa_dt as member_join_aaa_date,
member.join_club_dt as member_join_club_date ,
member.reason_joined as member_reason_joined_code ,
member.associate_relation_cd as member_associate_relation_code ,
member.solicitation_cd as member_solicitation_code ,
member.source_of_sale as member_source_of_sale_code,
member.commission_cd as commission_cd,
member.future_cancel_fl as member_future_cancel_indicator ,
member.future_cancel_dt as member_future_cancel_date ,
member.renew_method_cd as member_renew_method_code,
member.free_member_fl as free_member_indicator ,
member.previous_membership_id as previous_club_membership_identifier ,
member.previous_club_cd as member_previous_club_code,
crossref.previous_member_ky as merged_member_previous_key ,
crossref.previous_club_cd as merged_member_previous_club_code ,
crossref.previous_membership_id as merged_member_previous_membership_identifier ,
crossref.previous_associate_id as merged_member_previous_associate_identifier ,
crossref.previous_check_digit_nr as merged_member_previous_check_digit_number ,
crossref.previous_member16_id as merged_member_previous_member16_identifier ,
member.name_change_dt as member_name_change_datetime ,
member.adm_email_changed_dt as member_email_changed_datetime ,
member.bad_email_fl as member_bad_email_indicator ,
member.email_optout_fl as member_email_optout_indicator ,
member.web_last_login_dt as member_web_last_login_datetime ,
member.active_expiration_dt as member_active_expiration_date ,
member.activation_dt as member_activation_date ,
member_code.DNT_CODE as member_dn_text_indicator,
member_code.DEM_CODE as member_duplicate_email_indicator,
member_code.DNC_CODE as member_dn_call_indicator,
member_code.NOE_CODE as member_no_email_indicator,
member_code.DNM_CODE as member_dn_mail_indicator,
member_code.DNAE_CODE as member_dn_ask_for_email_indicator,
member_code.DNE_CODE as member_dn_email_indicator,
member_code.RFE_CODE as member_refused_give_email_indicator,
member.membership_purge_indicator as membership_purge_indicator,
CAST (member.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(member.membership_id,''),'|',
ifnull(member.associate_id,''),'|',
ifnull(member.check_digit_nr,''),'|',
ifnull(member.status,''),'|',
ifnull(member.member_expiration_dt,''),'|',
ifnull(member.member_type_cd,''),'|',
ifnull(member.do_not_renew_fl,''),'|',
ifnull(member.billing_cd,''),'|',
ifnull(member.billing_category_cd,''),'|',
ifnull(member.member_card_expiration_dt,''),'|',
ifnull(member.status_dt,''),'|',
ifnull(member.cancel_dt,''),'|',
ifnull(member.join_aaa_dt,''),'|',
ifnull(member.join_club_dt,''),'|',
ifnull(member.reason_joined,''),'|',
ifnull(member.associate_relation_cd,''),'|',
ifnull(member.solicitation_cd,''),'|',
ifnull(member.source_of_sale,''),'|',
ifnull(member.commission_cd,''),'|',
ifnull(member.future_cancel_fl,''),'|',
ifnull(member.future_cancel_dt,''),'|',
ifnull(member.renew_method_cd,''),'|',
ifnull(member.free_member_fl,''),'|',
ifnull(member.previous_membership_id,''),'|',
ifnull(member.previous_club_cd,''),'|',
ifnull(crossref.previous_member_ky,''),'|',
ifnull(crossref.previous_club_cd,''),'|',
ifnull(crossref.previous_membership_id,''),'|',
ifnull(crossref.previous_associate_id,''),'|',
ifnull(crossref.previous_check_digit_nr,''),'|',
ifnull(crossref.previous_member16_id,''),'|',
ifnull(member.name_change_dt,''),'|',
ifnull(member.adm_email_changed_dt,''),'|',
ifnull(member.bad_email_fl,''),'|',
ifnull(member.email_optout_fl,''),'|',
ifnull(member.web_last_login_dt,''),'|',
ifnull(member.active_expiration_dt,''),'|',
ifnull(member.activation_dt,''),'|',
ifnull(adw_memship.membership_adw_key,'-1'),'|',
ifnull(cust_src_key.contact_adw_key,'-1'),'|',
ifnull(solicitation.membership_solicitation_adw_key,'-1'),'|',
ifnull(role.employee_role_adw_key,'-1'),'|',
ifnull(SAFE_CAST(member_code.DNT_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.DEM_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.DNC_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.NOE_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.DNM_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.DNAE_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.DNE_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.RFE_CODE AS STRING),''),'|',
ifnull(member.membership_purge_indicator,''),'|'
))) as adw_row_hash

FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_source` AS member
    LEFT JOIN
    (
    SELECT
      member_ky,
      MAX(CASE
          WHEN code='DNT' THEN 1
        ELSE
        0
      END
        ) AS DNT_CODE,
      MAX(CASE
          WHEN code='DEM' THEN 1
        ELSE
        0
      END
        ) AS DEM_CODE,
      MAX(CASE
          WHEN code='DNC' THEN 1
        ELSE
        0
      END
        ) AS DNC_CODE,
      MAX(CASE
          WHEN code='NOE' THEN 1
        ELSE
        0
      END
        ) AS NOE_CODE,
      MAX(CASE
          WHEN code='DNM' THEN 1
        ELSE
        0
      END
        ) AS DNM_CODE,
      MAX(CASE
          WHEN code='DNAE' THEN 1
        ELSE
        0
      END
        ) AS DNAE_CODE,
      MAX(CASE
          WHEN code='DNE' THEN 1
        ELSE
        0
      END
        ) AS DNE_CODE,
      MAX(CASE
          WHEN code='RFE' THEN 1
        ELSE
        0
      END
        ) AS RFE_CODE
    FROM
      `{iu.INGESTION_PROJECT}.mzp.member_code`
    GROUP BY
      1) AS member_code
  ON
    member.member_ky = member_code.member_ky
    LEFT JOIN
      (SELECT membership_source_system_key,
              membership_adw_key,
              ROW_NUMBER() OVER (PARTITION BY membership_source_system_key) AS DUPE_CHECK
      FROM `{int_u.INTEGRATION_PROJECT}.member.dim_membership`
      WHERE active_indicator='Y'
      ) adw_memship 
      ON member.membership_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING) AND adw_memship.DUPE_CHECK=1
left join
`{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` cust_src_key
on member.member_ky=cust_src_key.source_1_key and cust_src_key.key_type_name like 'member_key%' and cust_src_key.active_indicator='Y'
left join ( select member_ky,
previous_member_ky,
previous_club_cd,
previous_membership_id,
previous_associate_id,
previous_check_digit_nr,
previous_member16_id,
ROW_NUMBER() OVER(PARTITION BY member_ky ORDER BY NULL DESC) AS DUPE_CHECK
from 
`{iu.INGESTION_PROJECT}.mzp.member_crossref`) crossref
on crossref.member_ky=member.member_ky and crossref.DUPE_CHECK=1
LEFT JOIN 
`{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role` role
on member.agent_id=role.employee_role_id and role.active_indicator='Y'
LEFT JOIN
(SELECT solicitation_code,
membership_solicitation_adw_key,
ROW_NUMBER() OVER (PARTITION BY solicitation_code) AS DUPE_CHECK
FROM `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation`
WHERE active_indicator='Y'
) solicitation
ON solicitation.solicitation_code=member.solicitation_cd and solicitation.DUPE_CHECK=1
where member.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

member_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_type_2_hist` AS
WITH
  member_hist AS (
  SELECT
    member_source_system_key ,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY member_source_system_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY member_source_system_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    member_source_system_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    member_hist 
  ), set_groups AS (
  SELECT
    member_source_system_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY member_source_system_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    member_source_system_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    member_source_system_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  member_source_system_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Member
member_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.dim_member`
  (
  member_adw_key,
membership_adw_key,
contact_adw_key,
membership_solicitation_adw_key,
employee_role_adw_key,
member_source_system_key,
membership_identifier,
member_associate_identifier,
member_check_digit_number,
member_status_code,
member_expiration_date,
member_card_expiration_date,
primary_member_indicator,
member_do_not_renew_indicator,
member_billing_code,
member_billing_category_code,
member_status_datetime,
member_cancel_date,
member_join_aaa_date,
member_join_club_date,
member_reason_joined_code,
member_associate_relation_code,
member_solicitation_code,
member_source_of_sale_code,
member_commission_code,
member_future_cancel_indicator,
member_future_cancel_date,
member_renew_method_code,
free_member_indicator,
previous_club_membership_identifier,
previous_club_code,
merged_member_previous_key,
merged_member_previous_club_code,
merged_member_previous_membership_identifier,
merged_member_previous_associate_identifier,
merged_member_previous_check_digit_number,
merged_member_previous_member16_identifier,
member_name_change_datetime,
member_email_changed_datetime,
member_bad_email_indicator,
member_email_optout_indicator,
member_web_last_login_datetime,
member_active_expiration_date,
member_activation_date,
member_dn_text_indicator,
member_duplicate_email_indicator,
member_dn_call_indicator,
member_no_email_indicator,
member_dn_mail_indicator,
member_dn_ask_for_email_indicator,
member_dn_email_indicator,
member_refused_give_email_indicator,
membership_purge_indicator,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
  ) 
SELECT
member_adw_key,
COALESCE(source.membership_adw_key,'-1') ,
COALESCE(source.contact_adw_key,'-1') ,
COALESCE(membership_solicitation_adw_key,'-1'),
COALESCE(source.employee_role_adw_key,'-1') ,
SAFE_CAST(source.member_source_system_key AS INT64) ,
source.membership_identifier ,
source.member_associate_identifier ,
SAFE_CAST(source.member_check_digit_number AS INT64) ,
source.member_status_code ,
CAST(substr(source.member_expiration_date,0,10) AS date) ,
CAST(substr(source.member_card_expiration_dt,0,10) AS date)  ,
source.primary_member_indicator ,
source.member_do_not_renew_indicator ,
source.member_billing_code ,
source.member_billing_category_code ,
CAST(source.member_status_datetime as datetime) ,
CAST(substr(source.member_cancel_date,0,10) AS date) ,
CASE WHEN SUBSTR(source.member_join_aaa_date,1,3) IN ('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
    THEN parse_date('%h %d %Y',substr(source.member_join_aaa_date,0,11))
    ELSE parse_date('%d-%b-%y',substr(source.member_join_aaa_date,0,10))
    END
,
CAST(substr(source.member_join_club_date,0,10) AS date) ,
source.member_reason_joined_code ,
source.member_associate_relation_code ,
source.member_solicitation_code ,
source.member_source_of_sale_code ,
source.commission_cd ,
source.member_future_cancel_indicator ,
CAST(substr(source.member_future_cancel_date,0,10) AS date) ,
source.member_renew_method_code ,
source.free_member_indicator ,
source.previous_club_membership_identifier ,
source.member_previous_club_code ,
safe_cast(source.merged_member_previous_key as int64) ,
source.merged_member_previous_club_code ,
source.merged_member_previous_membership_identifier ,
source.merged_member_previous_associate_identifier ,
source.merged_member_previous_check_digit_number ,
source.merged_member_previous_member16_identifier ,
SAFE_CAST(source.member_name_change_datetime AS DATETIME) ,
SAFE_CAST(source.member_email_changed_datetime AS DATETIME) ,
source.member_bad_email_indicator ,
source.member_email_optout_indicator ,
SAFE_CAST(source.member_web_last_login_datetime AS DATETIME) ,
CAST(substr (source.member_active_expiration_date,0,10) AS date) ,
CAST(substr (source.member_activation_date,0,10) AS date) ,
SAFE_CAST(source.member_dn_text_indicator AS STRING) ,
SAFE_CAST(source.member_duplicate_email_indicator AS STRING) ,
SAFE_CAST(source.member_dn_call_indicator AS STRING) ,
SAFE_CAST(source.member_no_email_indicator AS STRING) ,
SAFE_CAST(source.member_dn_mail_indicator AS STRING) ,
SAFE_CAST(source.member_dn_ask_for_email_indicator AS STRING) ,
SAFE_CAST(source.member_dn_email_indicator AS STRING) ,
SAFE_CAST(source.member_refused_give_email_indicator AS STRING) ,
SAFE_CAST(source.membership_purge_indicator AS STRING) ,
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
(CASE WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END) ,
source.adw_row_hash,
CURRENT_DATETIME() ,
1 ,
CURRENT_DATETIME() ,
1 
    FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_transformed` source
  ON (
  source.member_source_system_key=hist_type_2.member_source_system_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS member_adw_key,
    member_source_system_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_transformed`
  GROUP BY
    member_source_system_key) pk
ON (source.member_source_system_key=pk.member_source_system_key)"""

####################################################################################################################################
# Member_rider
####################################################################################################################################

#################################
# Member_rider - Stage Load
#################################
# Member_rider

######################################################################
# Load Member_rider Historical set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

member_rider_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_source` AS
SELECT
membership_ky,
agent_id,
rider_comp_cd,
AUTORENEWAL_CARD_KY,
member_ky,
rider.STATUS_DT  as member_rider_status_datetime
,rider.STATUS as member_rider_status_code
,RIDER_EFFECTIVE_DT  as member_rider_effective_date
,rider.CANCEL_DT  as member_rider_cancel_date
,CANCEL_REASON_CD as member_rider_cancel_reason_code
,rider.BILLING_CATEGORY_CD as member_rider_billing_category_code
,rider.SOLICITATION_CD  as member_rider_solicitation_code
,COST_EFFECTIVE_DT  as member_rider_cost_effective_date
,ACTUAL_CANCEL_DT  as member_rider_actual_cancel_datetime
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
,rider.last_upd_dt  , ROW_NUMBER() OVER (PARTITION BY RIDER_KY, last_upd_dt ORDER BY NULL DESC) AS dupe_check
 FROM `{iu.INGESTION_PROJECT}.mzp.rider` rider

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

member_rider_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_transformed` AS
SELECT 
coalesce(member_adw_key,'-1') as member_adw_key,
coalesce(membership.membership_adw_key,'-1') as membership_adw_key,
coalesce(product.product_adw_key,'-1') as product_adw_key,
coalesce(employee_role.employee_role_adw_key,'-1') as employee_role_adw_key,
member_rider_status_datetime
,member_rider_status_code
,member_rider_effective_date
,member_rider_cancel_date
,member_rider_cancel_reason_code
,member_rider_billing_category_code
,member_rider_solicitation_code
,member_rider_cost_effective_date
,member_rider_actual_cancel_datetime
,coalesce(auto_renewal.member_auto_renewal_card_adw_key,'-1') as member_auto_renewal_card_adw_key
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
,CAST (rider.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(COALESCE(member_rider_status_datetime,'')
,COALESCE(member_rider_status_code,'')
,COALESCE(member_rider_effective_date,'')
,COALESCE(member_rider_cancel_date,'')
,COALESCE(member_rider_cancel_reason_code,'')
,COALESCE(member_rider_billing_category_code,'')
,COALESCE(member_rider_solicitation_code,'')
,COALESCE(member_rider_cost_effective_date,'')
,COALESCE(member_rider_actual_cancel_datetime,'')
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
,COALESCE(member_adw_key,'-1')
,COALESCE(product.product_adw_key,'-1')
,COALESCE(auto_renewal.member_auto_renewal_card_adw_key,'-1')
,COALESCE(employee_role.employee_role_adw_key,'-1')
,COALESCE(membership.membership_adw_key,'-1')
))) as adw_row_hash
,
 ROW_NUMBER() OVER (PARTITION BY member_rider_source_key,last_upd_dt ORDER BY last_upd_dt DESC ) AS DUP_CHECK
FROM
 `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_source` AS rider
left  join 
`{int_u.INTEGRATION_PROJECT}.member.dim_member` member 
on 
SAFE_CAST(rider.member_ky as INT64)=member.member_source_system_key and member.active_indicator='Y' 
left  join `{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership on SAFE_CAST(rider.membership_ky as INT64) =membership.membership_source_system_key and membership.active_indicator='Y'
left  join `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role` employee_role on employee_role.employee_role_id=rider.agent_id and employee_role.active_indicator='Y'
left join `{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` auto_renewal on auto_renewal.member_source_arc_key=CAST(rider.AUTORENEWAL_CARD_KY as INT64) and auto_renewal.active_indicator='Y'
left  join (select product.* from `{int_u.INTEGRATION_PROJECT}.adw.dim_product` product join `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` dim_product_category ON product_category_code IN  ('RCC','MBR_FEES') and dim_product_category.product_category_adw_key =product.product_category_adw_key  ) product
on rider.rider_comp_cd=product.product_sku_key and product.active_indicator='Y' where rider.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

member_rider_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_type_2_hist` AS
WITH
  members_rider_hist AS (
  SELECT
    member_rider_source_key,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY member_rider_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY member_rider_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    member_rider_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    members_rider_hist 
  ), set_groups AS (
  SELECT
    member_rider_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY member_rider_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    member_rider_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    member_rider_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  member_rider_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Member_rider
member_rider_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

INSERT INTO
`{int_u.INTEGRATION_PROJECT}.member.dim_member_rider`
(member_rider_adw_key,	
membership_adw_key,	
member_adw_key,	
member_auto_renewal_card_adw_key,	
employee_role_adw_key,	
product_adw_key,	
member_rider_source_key,	
member_rider_status_code,	
member_rider_status_datetime,	
member_rider_cancel_date,	
member_rider_cost_effective_date,	
member_rider_solicitation_code,	
member_rider_do_not_renew_indicator,	
member_rider_dues_cost_amount,	
member_rider_dues_adjustment_amount,	
member_rider_payment_amount,	
member_rider_paid_by_code,	
member_rider_future_cancel_date,	
member_rider_billing_category_code,	
member_rider_cancel_reason_code,	
member_rider_reinstate_indicator,	
member_rider_effective_date,	
member_rider_adm_original_cost_amount,	
member_rider_reinstate_reason_code,	
member_rider_extend_expiration_amount,	
member_rider_actual_cancel_datetime,	
member_rider_definition_key,	
member_rider_activation_date,	
effective_start_datetime,	
effective_end_datetime,	
active_indicator,	
adw_row_hash,	
integrate_insert_datetime,	
integrate_insert_batch_number,	
integrate_update_datetime,	
integrate_update_batch_number)
SELECT
member_rider_adw_key,	
SAFE_CAST(membership_adw_key as STRING),	
SAFE_CAST(member_adw_key as STRING),	
member_auto_renewal_card_adw_key,	
employee_role_adw_key,	
product_adw_key,	
SAFE_CAST(source.member_rider_source_key AS INT64),	
member_rider_status_code,	
SAFE_CAST(member_rider_status_datetime as DATETIME),	
SAFE_CAST(TIMESTAMP(member_rider_cancel_date) as DATE),	
SAFE_CAST(TIMESTAMP(member_rider_cost_effective_date) as DATE),	
member_rider_solicitation_code,	
member_rider_do_not_renew_indicator,	
SAFE_CAST(member_rider_dues_cost_amount as NUMERIC),	
SAFE_CAST(member_rider_dues_adjustment_amount as NUMERIC),	
SAFE_CAST(member_rider_payment_amount as NUMERIC),	
member_rider_paid_by_code,	
SAFE_CAST(TIMESTAMP(member_rider_future_cancel_date) as DATE),	
member_rider_billing_category_code,	
member_rider_cancel_reason_code,	
member_rider_reinstate_indicator,	
SAFE_CAST(TIMESTAMP(member_rider_effective_date) as DATE),	
SAFE_CAST(member_rider_adm_original_cost_amount as NUMERIC),	
member_rider_reinstate_reason_code,	
SAFE_CAST(member_rider_extend_expiration_amount as NUMERIC),	
SAFE_CAST(member_rider_actual_cancel_datetime as DATETIME),	
SAFE_CAST(member_rider_definition_key as INT64),	
SAFE_CAST(TIMESTAMP(member_rider_activation_date) as DATE),	
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
CASE WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'ELSE 'N' END AS active_indicator,
source.adw_row_hash,
CURRENT_DATETIME(),
1,
CURRENT_DATETIME(),
1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_transformed` source ON (
  source.member_rider_source_key=hist_type_2.member_rider_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS member_rider_adw_key,
    member_rider_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_transformed`
  GROUP BY
    member_rider_source_key) pk
ON (source.member_rider_source_key=pk.member_rider_source_key)

"""
######################################################################
# Load Employee_role Historical set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

employee_role_work_source = f"""
-- employee_role_work_source
-- Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_source` AS
SELECT
   employee_role_source.agent_id,
   employee_role_source.branch_ky,
   employee_role_source.first_name ,
   employee_role_source.last_name ,
   employee_role_source.user_id,
   employee_role_source.agent_type_cd,
  employee_role_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY employee_role_source.agent_id,employee_role_source.last_upd_dt ORDER BY NULL DESC ) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.sales_agent` AS employee_role_source

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

employee_role_work_transformed = f"""
-- employee_role_work_transformed
-- Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_transformed` AS
SELECT
  COALESCE(emp.employee_adw_key,
    "-1") AS employee_adw_key,
  COALESCE(name1.name_adw_key,
    TO_BASE64(MD5(CONCAT(ifnull(employee_role.first_name,
            ''),'|',ifnull(employee_role.last_name,
            ''))))) AS name_adw_key,
  COALESCE(office.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  'Membership' AS employee_role_business_line_code,
  employee_role.agent_id AS employee_role_id,
  employee_role.agent_type_cd AS employee_role_type,
  employee_role.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT(ifnull(employee_role.agent_id,
          ''),'|',ifnull(employee_role.agent_type_cd,
          '')))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_source` AS employee_role
LEFT OUTER JOIN(select 
     user_id, USERNAME,
     ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY null ) AS dupe_check
   from `{iu.INGESTION_PROJECT}.mzp.cx_iusers`) users
ON
  employee_role.user_id=users.user_id  and  users.dupe_check=1
LEFT OUTER JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` emp
ON
  emp. employee_active_directory_user_identifier =users.username
  AND emp.active_indicator='Y'
LEFT OUTER JOIN
 ( SELECT
    branch_ky,branch_cd,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_KY ORDER BY LAST_UPD_DT DESC) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.branch`  ) branch
ON
  employee_role.branch_ky=branch.branch_ky and branch.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    membership_branch_code,
    active_indicator,
    ROW_NUMBER() OVER(PARTITION BY membership_branch_code ORDER BY effective_start_datetime DESC) AS dupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` ) office
ON
  branch.branch_cd =office.membership_branch_code
  AND office.dupe_check = 1
  AND office.active_indicator='Y'
LEFT OUTER JOIN (
  SELECT
    name.name_adw_key,
    name.raw_first_name,
    name. raw_last_name,
    xref.effective_start_datetime,
    ROW_NUMBER() OVER(PARTITION BY name. raw_first_name, name. raw_last_name ORDER BY xref.effective_start_datetime DESC) AS dupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name,
    `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name` xref
  WHERE
    name.name_adw_key=xref.name_adw_key) name1
ON
  name1. raw_first_name = employee_role.first_name
  AND name1. raw_last_name = employee_role.last_name
  AND name1.dupe_check=1
WHERE
  employee_role.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

employee_role_work_type2_logic = f"""
-- employee_role_work_type2_logic
-- Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_type_2_hist` AS
WITH
  employee_role_hist AS (
  SELECT
    employee_role_id,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY employee_role_id ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(cast(effective_start_datetime as datetime)) OVER (PARTITION BY employee_role_id ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    employee_role_id,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    employee_role_hist 
  ), set_groups AS (
  SELECT
    employee_role_id,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY employee_role_id ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    employee_role_id,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    employee_role_id,
    adw_row_hash,
    grouping_column 
  )
SELECT
  employee_role_id,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# employee_role
employee_role_insert = f"""
-- employee_role_insert
-- Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role`
  (
employee_role_adw_key,	
employee_adw_key,	
name_adw_key,	
aca_office_adw_key,	
employee_role__business_line_code,	
employee_role_id,	
employee_role_type,	
effective_start_datetime,	
effective_end_datetime,	
active_indicator,
adw_row_hash ,
integrate_insert_datetime,	
integrate_insert_batch_number,	
integrate_update_datetime,	
integrate_update_batch_number
)
SELECT
  employee_role_adw_key ,
  source.employee_adw_key,
  source. name_adw_key ,
  source.aca_office_adw_key,
  source. employee_role_business_line_code,
  source. employee_role_id ,
  source. employee_role_type ,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_transformed` source ON (
  source.employee_role_id=hist_type_2.employee_role_id
  AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS employee_role_adw_key ,
    employee_role_id
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_transformed`
  GROUP BY
    employee_role_id) pk
ON (source.employee_role_id=pk.employee_role_id) 

"""

###########################################################################################################################
# Membership segment
##########################################################################################################################
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
,ROW_NUMBER() OVER(PARTITION BY historysource.segmentation_history_ky, historysource.membership_exp_dt ORDER BY NULL DESC) AS dupe_check
FROM `{iu.INGESTION_PROJECT}.mzp.segmentation_history` as historysource 
JOIN (select 
      *,
      row_number() over (partition by name order by segmentation_test_end_dt desc) as rn	  
      from `{iu.INGESTION_PROJECT}.mzp.segmentation_setup` ) setupsource on (historysource.segmentation_setup_ky=setupsource.segmentation_setup_ky and setupsource.rn=1)
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
 ,  concat(ifnull(membership_ky,''),'|', ifnull(membership_exp_dt,''), '|', ifnull(name,'')) as segment_key
 ,  CAST('2010-01-01' AS datetime)   as effective_start_datetime
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

membership_segment_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_type_2_hist` AS
WITH
  segment_hist AS (
  SELECT
    segmentation_history_ky,
    adw_row_hash,
    effective_start_datetime,
    LAG(effective_start_datetime) OVER (PARTITION BY segmentation_history_ky ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(effective_start_datetime) OVER (PARTITION BY segmentation_history_ky ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` 
  ), set_grouping_column AS (
    SELECT
    segmentation_history_ky,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR SAFE_CAST(prev_row_hash as STRING) <> SAFE_CAST(adw_row_hash as STRING) THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    segment_hist 
  ), set_groups AS (
  SELECT
    segmentation_history_ky,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY segmentation_history_ky ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    segmentation_history_ky,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    segmentation_history_ky,
    adw_row_hash,
    grouping_column 
  )
SELECT
  segmentation_history_ky,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

membership_segment_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation` 
  (
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
  )
SELECT
  membership_marketing_segmentation_adw_key
  ,source.membership_adw_key
  ,source.member_expiration_dt
  ,source.segmentation_test_group_indicator
  ,source.segmentation_panel_code
  ,source.segmentation_control_panel_indicator
  ,source.segmentation_commission_code
  ,source.segment_name
  ,source.adw_row_hash
  ,CAST(hist_type_2.effective_start_datetime AS datetime)
  ,CAST(hist_type_2.effective_end_datetime AS datetime)
  ,CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator
  , CURRENT_DATETIME()
  , 1
  , CURRENT_DATETIME()
  , 1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` source ON (
  source.segmentation_history_ky=hist_type_2.segmentation_history_ky
  AND SAFE_CAST(source.effective_start_datetime as STRING)=SAFE_CAST(hist_type_2.effective_start_datetime as STRING) )
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_marketing_segmentation_adw_key,
    segmentation_history_ky
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed`
  GROUP BY
    segmentation_history_ky) pk
ON (source.segmentation_history_ky=pk.segmentation_history_ky)

"""

####################################################################################################################################
# member_auto_renewal_card_key
####################################################################################################################################

#################################
# member_auto_renewal_card_key - Stage Load
#################################
# member_auto_renewal_card_key

######################################################################
# Load member_auto_renewal_card_key History set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

member_auto_renewal_card_work_source = f"""
CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_source` AS
SELECT
cc_first_name,
cc_last_name,
CC_STREET ,
CC_City ,
CC_State,
CC_Zip,
AUTORENEWAL_CARD_KY  as member_source_arc_key ,
STATUS_DT as member_arc_status_datetime,
CC_TYPE_CD as  member_cc_type_code,
CC_EXPIRATION_DT as member_cc_expiration_date ,
CC_REJECT_REASON_CD as member_cc_reject_reason_code,
CC_REJECT_DT as member_cc_reject_date,
DONOR_NR as member_cc_donor_number,
CC_LAST_FOUR as member_cc_last_four,
CREDIT_DEBIT_TYPE as member_credit_debit_type_code,	
DATETIME('2010-01-01') as last_upd_dt,
ROW_NUMBER() OVER (PARTITION BY AUTORENEWAL_CARD_KY,DATETIME('2010-01-01') ORDER BY NULL  DESC ) AS DUP_CHECK
  FROM
    `{iu.INGESTION_PROJECT}.mzp.autorenewal_card` autorenewal
"""

######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

member_auto_renewal_card_work_transformed = f"""

CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_transformed` AS
SELECT
`{int_u.INTEGRATION_PROJECT}.udfs.hash_name`(autorenewal.cc_first_name,'',autorenewal.cc_last_name,'','') as name_adw_key,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_address`(autorenewal.CC_STREET,'','',autorenewal.CC_City,autorenewal.CC_State,autorenewal.CC_Zip,'','') as address_adw_key,
member_source_arc_key ,
member_arc_status_datetime,
member_cc_type_code,
member_cc_expiration_date ,
member_cc_reject_reason_code,
member_cc_reject_date,
member_cc_donor_number,
member_cc_last_four,
member_credit_debit_type_code,
CAST (autorenewal.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(
COALESCE(member_source_arc_key ,''),
COALESCE(member_arc_status_datetime,''),
COALESCE(member_cc_type_code,''),
COALESCE(member_cc_expiration_date ,''),
COALESCE(member_cc_reject_reason_code,''),
COALESCE(member_cc_reject_date,''),
COALESCE(member_cc_donor_number,''),
COALESCE(member_cc_last_four,''),
COALESCE(member_credit_debit_type_code,'')
))) as adw_row_hash
,
 ROW_NUMBER() OVER (PARTITION BY member_source_arc_key ORDER BY last_upd_dt DESC ) AS DUP_CHECK
FROM
 `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_source` autorenewal where autorenewal.dup_check=1

"""

######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

member_auto_renewal_card_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_Work_type_2_hist` AS
WITH
  member_auto_renewal_card_key_hist AS (
  SELECT
    member_source_arc_key,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY member_source_arc_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY member_source_arc_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_transformed`
  ), set_grouping_column AS (
  SELECT
    member_source_arc_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    member_auto_renewal_card_key_hist 
  ), set_groups AS (
  SELECT
    member_source_arc_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY member_source_arc_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    member_source_arc_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    member_source_arc_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  member_source_arc_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

#################################
# Load Data warehouse tables from prepared data
#################################

## dim_member_auto_renewal_card_key
member_auto_renewal_card_insert = f"""

INSERT INTO
`{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card`
(member_auto_renewal_card_adw_key,
name_adw_key			     ,
address_adw_key			     ,
member_source_arc_key		 ,
member_arc_status_datetime	, 
member_cc_type_code			 ,
member_cc_expiration_date	, 
member_cc_reject_reason_code,
member_cc_reject_date		 ,
member_cc_donor_number		, 
member_cc_last_four			 ,
member_credit_debit_type_code,		
effective_start_datetime	, 
effective_end_datetime		 ,
active_indicator			 ,
adw_row_hash			     ,
integrate_insert_datetime	, 
integrate_insert_batch_number,	
integrate_update_datetime	, 
integrate_update_batch_number)
SELECT
SAFE_CAST(member_auto_renewal_card_adw_key as STRING),
SAFE_CAST(name_adw_key			  as STRING)   ,
SAFE_CAST(address_adw_key			   as STRING)  ,
SAFE_CAST(hist_type_2.member_source_arc_key as INT64)		 ,
SAFE_CAST(member_arc_status_datetime as DATETIME)	, 
member_cc_type_code			 ,
SAFE_CAST(TIMESTAMP(member_cc_expiration_date) as DATE), 
member_cc_reject_reason_code,
SAFE_CAST(TIMESTAMP(member_cc_reject_date) as DATE) ,
member_cc_donor_number		, 
member_cc_last_four			 ,
member_credit_debit_type_code,		
CAST(hist_type_2.effective_start_datetime AS datetime) as effective_start_datetime,
CAST(hist_type_2.effective_end_datetime AS datetime) as effective_end_datetime , 
CASE WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END AS active_indicator,
source.adw_row_hash,
CURRENT_DATETIME(),
1,
CURRENT_DATETIME(),
1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_Work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_transformed` source ON (
  source.member_source_arc_key=hist_type_2.member_source_arc_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS member_auto_renewal_card_adw_key	,
    member_source_arc_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_transformed`
  GROUP BY
    member_source_arc_key) pk
ON (source.member_source_arc_key=pk.member_source_arc_key)

"""

####################################################################################################################################
# Billing_Summary_hist
####################################################################################################################################
######################################################################
# Load Billing_Summary Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_billing_summary_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_source` AS
  SELECT
 billing_summary_source.bill_summary_ky as bill_summary_source_key,
 billing_summary_source.membership_ky ,
 billing_summary_source.process_dt as bill_process_date,
 billing_summary_source.notice_nr as bill_notice_number,
 billing_summary_source.bill_at as bill_amount,
 billing_summary_source.credit_at as bill_credit_amount,
 billing_summary_source.bill_type,
 billing_summary_source.expiration_dt as member_expiration_date,
 billing_summary_source.payment_at as bill_paid_amount,
 billing_summary_source.renew_method_cd as bill_renewal_method,
 billing_summary_source.bill_panel as bill_panel_code,
 billing_summary_source.ebill_fl as bill_ebilling_indicator,
 billing_summary_source.last_upd_dt,
 ROW_NUMBER() OVER(PARTITION BY billing_summary_source.bill_summary_ky ORDER BY billing_summary_source.last_upd_dt DESC) AS dupe_check
   FROM
   `{iu.INGESTION_PROJECT}.mzp.bill_summary` AS billing_summary_source
 WHERE
  billing_summary_source.last_upd_dt IS NOT NULL

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_billing_summary_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` AS
 SELECT
 adw_memship.membership_adw_key as membership_adw_key,
 billing_summary_source.bill_summary_source_key,
 billing_summary_source.bill_process_date,
 billing_summary_source.bill_notice_number,
 billing_summary_source.bill_amount,
 billing_summary_source.bill_credit_amount,
 billing_summary_source.bill_type,
 billing_summary_source.member_expiration_date,
 billing_summary_source.bill_paid_amount,
 billing_summary_source.bill_renewal_method,
 billing_summary_source.bill_panel_code,
 billing_summary_source.bill_ebilling_indicator,
 CAST (billing_summary_source.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(adw_memship.membership_adw_key,''),'|',
ifnull(billing_summary_source.bill_summary_source_key,''),'|',
ifnull(billing_summary_source.bill_process_date,''),'|',
ifnull(billing_summary_source.bill_notice_number,''),'|',
ifnull(billing_summary_source.bill_amount,''),'|',
ifnull(billing_summary_source.bill_credit_amount,''),'|',
ifnull(billing_summary_source.bill_type,''),'|',
ifnull(billing_summary_source.member_expiration_date,''),'|',
ifnull(billing_summary_source.bill_paid_amount,''),'|',
ifnull(billing_summary_source.bill_renewal_method,''),'|',
ifnull(billing_summary_source.bill_panel_code,''),'|',
ifnull(billing_summary_source.bill_ebilling_indicator,''),'|'
))) as adw_row_hash

FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_source` AS billing_summary_source
    LEFT JOIN
     `{int_u.INTEGRATION_PROJECT}.member.dim_membership` adw_memship
 on billing_summary_source.membership_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING) AND active_indicator='Y'
 where dupe_check = 1


"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_summary_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_type_2_hist` AS
WITH
  membership_billing_summary_hist AS (
  SELECT
    bill_summary_source_key ,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY bill_summary_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY bill_summary_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    bill_summary_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    membership_billing_summary_hist 
  ), set_groups AS (
  SELECT
    bill_summary_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY bill_summary_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    bill_summary_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    bill_summary_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  bill_summary_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_billing_summary
membership_billing_summary_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary`
  (
membership_billing_summary_adw_key,
membership_adw_key,
bill_summary_source_key,
bill_process_date,
bill_notice_number,
bill_amount,
bill_credit_amount,
bill_type,
member_expiration_date,
bill_paid_amount,
bill_renewal_method,
bill_panel_code,
bill_ebilling_indicator,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
  ) 
SELECT
membership_billing_summary_adw_key,
COALESCE(source.membership_adw_key,'-1') ,
SAFE_CAST(source.bill_summary_source_key AS INT64),
CAST(substr (source.bill_process_date,0,10) AS Date),
CAST(source.bill_notice_number AS INT64),
CAST(source.bill_amount AS NUMERIC),
CAST(source.bill_credit_amount AS NUMERIC),
source.bill_type,
CAST(substr (source.member_expiration_date,0,10) AS Date),
CAST(source.bill_paid_amount AS NUMERIC),
source.bill_renewal_method,
source.bill_panel_code,
source.bill_ebilling_indicator,
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
( CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END) ,
source.adw_row_hash,
CURRENT_DATETIME() ,
1 ,
CURRENT_DATETIME() ,
1 
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` source
  ON (
  source.bill_summary_source_key=hist_type_2.bill_summary_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_billing_summary_adw_key,
    bill_summary_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed`
  GROUP BY
    bill_summary_source_key) pk
ON (source.bill_summary_source_key=pk.bill_summary_source_key)


"""

####################################################################################################################################
# Billing_detail_hist
####################################################################################################################################
######################################################################
# Load Billing_detail Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_billing_detail_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_source` AS
  SELECT
 billing_detail_source.bill_detail_ky as bill_detail_source_key,
 billing_detail_source.bill_summary_ky as bill_summary_source_key,
 billing_detail_source.member_ky ,
 billing_detail_source.rider_ky ,
 billing_detail_source.membership_fees_ky,
 billing_detail_source.bill_detail_at as bill_detail_amount,
 billing_detail_source.detail_text as bill_detail_text,
 billing_detail_source.billing_category_cd as rider_billing_category_code,
 billing_detail_source.solicitation_cd as rider_solicitation_code,
 billing_detail_source.last_upd_dt,
 ROW_NUMBER() OVER(PARTITION BY billing_detail_source.bill_detail_ky ORDER BY billing_detail_source.last_upd_dt DESC) AS dupe_check
   FROM
   `{iu.INGESTION_PROJECT}.mzp.bill_detail` AS billing_detail_source
 WHERE
  billing_detail_source.last_upd_dt IS NOT NULL

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_billing_detail_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` AS
 SELECT
 billing_summary.membership_billing_summary_adw_key,
 billing_summary.membership_adw_key,
 adw_mem.member_adw_key,
 adw_product.product_adw_key,
 adw_membership_fee.membership_fee_adw_key,
 billing_detail_source.bill_detail_source_key,
 billing_detail_source.bill_summary_source_key,
 billing_detail_source.bill_detail_amount,
 billing_summary.bill_process_date, 
 billing_summary.bill_notice_number,
 billing_summary.bill_type,
 billing_detail_source.bill_detail_text,
 billing_detail_source.rider_billing_category_code,
 adw_cx_codes.code_desc as rider_billing_category_description,
 billing_detail_source.rider_solicitation_code,
 CAST (billing_detail_source.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(billing_summary.membership_adw_key,''),'|',
ifnull(adw_mem.member_adw_key,''),'|',
ifnull(adw_membership_fee.membership_fee_adw_key,''),'|',
ifnull(billing_detail_source.bill_detail_source_key,''),'|',
ifnull(billing_detail_source.bill_summary_source_key,''),'|',
ifnull(billing_detail_source.bill_detail_amount,''),'|',
ifnull(CAST(billing_summary.bill_process_date AS STRING),''),'|',
ifnull(CAST(billing_summary.bill_notice_number AS STRING),''),'|',
ifnull(billing_summary.bill_type,''),'|',
ifnull(billing_detail_source.bill_detail_text,''),'|',
ifnull(billing_detail_source.rider_billing_category_code,''),'|',
ifnull(billing_detail_source.rider_solicitation_code,''),'|',
ifnull(adw_cx_codes.code_desc,''),'|',
ifnull(billing_detail_source.rider_solicitation_code,''),'|'
))) as adw_row_hash

FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_source` AS billing_detail_source LEFT JOIN 
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` AS billing_summary 
                                on billing_detail_source.bill_summary_source_key =SAFE_CAST(billing_summary.bill_summary_source_key AS STRING) AND active_indicator='Y' LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` adw_mem
                                on billing_detail_source.member_ky =SAFE_CAST(adw_mem.member_source_system_key AS STRING) AND adw_mem.active_indicator='Y' LEFT JOIN
    (SELECT rider_ky, rider_comp_cd , ROW_NUMBER() OVER(PARTITION BY rider_ky ORDER BY last_upd_dt DESC) AS latest_rec
     FROM `{iu.INGESTION_PROJECT}.mzp.rider`) adw_rider
                                on billing_detail_source.rider_ky = adw_rider.rider_ky and adw_rider.latest_rec = 1 LEFT JOIN
    (select a.product_adw_key, a.product_sku_key
     from `{int_u.INTEGRATION_PROJECT}.adw.dim_product` a JOIN `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` b
     on a.product_category_adw_key = b.product_category_adw_key
     where b.product_category_code = 'RCC' 
     AND a.active_indicator='Y'
     AND b.active_indicator='Y') adw_product 
                                on adw_rider.rider_comp_cd = adw_product.product_sku_key LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` adw_membership_fee
                                on billing_detail_source.membership_fees_ky =SAFE_CAST(adw_membership_fee.membership_fee_source_key AS STRING) AND adw_membership_fee.active_indicator='Y'  LEFT JOIN 
    (SELECT code, code_desc, ROW_NUMBER() OVER(PARTITION BY code_ky ORDER BY last_upd_dt DESC) AS latest_rec
     FROM `{iu.INGESTION_PROJECT}.mzp.cx_codes` 
     where code_type = 'BILTYP') adw_cx_codes
                                on billing_detail_source.rider_billing_category_code =adw_cx_codes.code AND adw_cx_codes.latest_rec = 1
 where dupe_check = 1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_detail_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_type_2_hist` AS
WITH
  membership_billing_detail_hist AS (
  SELECT
    bill_detail_source_key ,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY bill_detail_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY bill_detail_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    bill_detail_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    membership_billing_detail_hist 
  ), set_groups AS (
  SELECT
    bill_detail_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY bill_detail_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    bill_detail_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    bill_detail_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  bill_detail_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_billing_detail
membership_billing_detail_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail`
  (
membership_billing_detail_adw_key,
membership_billing_summary_adw_key,
member_adw_key,
product_adw_key,
bill_detail_source_key,
bill_summary_source_key,
bill_process_date,
bill_notice_number,
bill_type,
bill_detail_amount,
bill_detail_text,
rider_billing_category_code,
rider_billing_category_description,
rider_solicitation_code,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
  ) 
SELECT
membership_billing_detail_adw_key,
source.membership_billing_summary_adw_key,
COALESCE(source.member_adw_key,'-1') ,
COALESCE(source.product_adw_key,'-1') ,
SAFE_CAST(source.bill_detail_source_key AS INT64),
SAFE_CAST(source.bill_summary_source_key AS INT64),
source.bill_process_date,
CAST(source.bill_notice_number AS INT64),
source.bill_type,
CAST(source.bill_detail_amount AS NUMERIC),
source.bill_detail_text,
source.rider_billing_category_code,
source.rider_billing_category_description,
source.rider_solicitation_code,
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
( CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END) ,
source.adw_row_hash,
CURRENT_DATETIME() ,
1 ,
CURRENT_DATETIME() ,
1 
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` source
  ON (
  source.bill_detail_source_key=hist_type_2.bill_detail_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_billing_detail_adw_key,
    bill_detail_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed`
  GROUP BY
    bill_detail_source_key) pk
ON (source.bill_detail_source_key=pk.bill_detail_source_key)

"""

####################################################################################################################################
# Payment summary
####################################################################################################################################


payment_applied_summary_hist_source = f"""
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
,ROW_NUMBER() OVER(PARTITION BY membership_payment_ky, last_upd_dt ORDER BY  NULL DESC) AS dupe_check
FROM `{iu.INGESTION_PROJECT}.mzp.payment_summary` 
"""
payment_applied_summary_hist_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` as 
SELECT 
 SAFE_CAST(membership_payment_ky as int64)  as payment_applied_source_key 
,COALESCE(aca_office.aca_office_adw_key,'-1') as aca_office_adw_key
,COALESCE(membership.membership_adw_key,'-1') as membership_adw_key
,SAFE_CAST (SAFE_CAST(payment_dt as datetime) as date) as payment_applied_date 
,SAFE_CAST(payment_at as numeric)  as payment_applied_amount
,batch_name as payment_batch_name
,source.payment_method_cd as payment_method_code
,cx_codes1.code_desc as payment_method_description
,source.transaction_type_cd as payment_type_code
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
,COALESCE( source.payment_method_cd,'')
,COALESCE( source.transaction_type_cd,'')
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
LEFT JOIN    
  (select max(batch_payment_ky) as batch_payment_ky,batch_ky,membership_ky,transaction_type_cd,payment_method_cd
          from `{iu.INGESTION_PROJECT}.mzp.batch_payment` group by batch_ky,membership_ky,transaction_type_cd,payment_method_cd) batch_payment      ON (source.batch_ky=batch_payment.batch_ky and 
      source.membership_ky=batch_payment.membership_ky and 
      source.transaction_type_cd=batch_payment.transaction_type_cd and 
      source.payment_method_cd=batch_payment.payment_method_cd )   
LEFT JOIN (select max(membership_payments_received_adw_key) as membership_payments_received_adw_key,
                  payment_received_source_key,active_indicator 
          from `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received`
          group by payment_received_source_key,active_indicator) payments_received 
          ON SAFE_CAST(batch_payment.batch_payment_ky as INT64)=payments_received.payment_received_source_key 
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

payment_applied_summary_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_work_type_2_hist` AS
WITH
  payment_summary_hist AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY payment_applied_source_key ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(effective_start_datetime) OVER (PARTITION BY payment_applied_source_key ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` 
  ), set_grouping_column AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR SAFE_CAST(prev_row_hash as STRING) <> SAFE_CAST(adw_row_hash as STRING) THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    payment_summary_hist 
  ), set_groups AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY payment_applied_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    payment_applied_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  payment_applied_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""
payment_applied_summary_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` 
( 
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
,adw_row_hash
,effective_start_datetime
,effective_end_datetime
,active_indicator
,integrate_insert_datetime
,integrate_insert_batch_number
,integrate_update_datetime
,integrate_update_batch_number
)
SELECT
membership_payment_applied_summary_adw_key
,membership_payment_applied_reversal_adw_key
,source.payment_applied_source_key
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
,source.adw_row_hash
,hist_type_2.effective_start_datetime
,hist_type_2.effective_end_datetime
,CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator
 ,CURRENT_DATETIME()
 ,1
 ,CURRENT_DATETIME()
 ,1
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` source ON (
  source.payment_applied_source_key=hist_type_2.payment_applied_source_key
  AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_payment_applied_summary_adw_key,
    payment_applied_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed`
  GROUP BY
    payment_applied_source_key) pk
ON (source.payment_applied_source_key=pk.payment_applied_source_key)

"""

######################################################################
# Load Product Historical set of Data
######################################################################

membership_payments_received_work_source = f"""
--Replace {int_u.INTEGRATION_PROJECT}. with {int_u.INTEGRATION_PROJECT}.
-- Replace {iu.INGESTION_PROJECT}.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_payments_received_work_source` AS
  SELECT
  batch_payment.MEMBERSHIP_KY,
  batch_payment.BILL_KY,
  batch_header.BRANCH_KY,
  batch_header.USER_ID,
  batch_payment.AUTORENEWAL_CARD_KY,
  batch_payment.BATCH_KY,
  batch_payment.BATCH_PAYMENT_KY,
  batch_header.BATCH_NAME,
  batch_header.EXP_CT,
  batch_header.EXP_AT,
  batch_payment.CREATE_DT,
  batch_header.STATUS,
  batch_header.STATUS_DT,
  batch_payment.PAYMENT_AT,
  batch_payment.PAYMENT_METHOD_CD,
batch_payment.PAYMENT_METHOD_CD AS payment_received_method_description,
batch_payment.PAYMENT_SOURCE_CD,
batch_payment.PAYMENT_SOURCE_CD AS payment_source_description,
batch_payment.TRANSACTION_TYPE_CD,
batch_payment.TRANSACTION_TYPE_CD AS payment_type_description,
batch_payment.REASON_CD,
CASE WHEN batch_payment.PAID_BY_CD='P' THEN 'Payment' 
     ELSE (CASE WHEN batch_payment.PAID_BY_CD='D' THEN 'Discount' 
                ELSE (CASE WHEN batch_payment.PAID_BY_CD='V' THEN 'Voucher'
                           ELSE NULL
                           END
                      )END
          ) END AS PAID_BY_CD,
batch_payment.ADJUSTMENT_DESCRIPTION_CD,
batch_payment.POST_COMPLETED_FL,
batch_payment.POST_DT,
batch_payment.AUTO_RENEW_FL,
batch_payment.LAST_UPD_DT,
ROW_NUMBER() OVER(PARTITION BY batch_payment.BATCH_PAYMENT_KY,batch_payment.last_upd_dt ORDER BY NULL DESC) AS dupe_check
  FROM `{iu.INGESTION_PROJECT}.mzp.batch_payment` batch_payment,
   `{iu.INGESTION_PROJECT}.mzp.batch_header` batch_header
  where batch_header.BATCH_KY=batch_payment.BATCH_KY

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_payments_received_work_transformed = f"""

CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` AS
SELECT
COALESCE(membership.membership_adw_key,'-1') membership_adw_key,
COALESCE(bill_summary.membership_billing_summary_adw_key,'-1') membership_billing_summary_adw_key,
COALESCE(renewal.member_auto_renewal_card_adw_key,'-1') member_auto_renewal_card_adw_key,
COALESCE(branch1.aca_office_adw_key,'-1') aca_office_adw_key,
COALESCE(users1.employee_adw_key,'-1') employee_adw_key,
payments.BATCH_KY,
payments.BATCH_PAYMENT_KY,
payments.BATCH_NAME,
payments.EXP_CT,
payments.EXP_AT,
payments.CREATE_DT,
payments.STATUS,
payments.STATUS_DT,
payments.PAYMENT_AT,
payments.PAYMENT_METHOD_CD,
CODE_PEMTH.CODE_DESC AS payment_received_method_description,
payments.PAYMENT_SOURCE_CD,
CODE_PAYSRC.CODE_DESC AS payment_source_description,
payments.TRANSACTION_TYPE_CD,
CODE_PAYSRC_TRANSACTION.CODE_DESC AS payment_type_description,
payments.REASON_CD,
payments.PAID_BY_CD,
payments.ADJUSTMENT_DESCRIPTION_CD,
CODE_PAYADJ.CODE_DESC AS payment_adjustment_description,
payments.POST_COMPLETED_FL,
payments.POST_DT,
payments.AUTO_RENEW_FL,
payments.LAST_UPD_DT,
TO_BASE64(MD5(CONCAT(ifnull(COALESCE(membership.membership_adw_key,'-1'),''),'|',
ifnull(COALESCE(bill_summary.membership_billing_summary_adw_key,'-1'),''),'|',
ifnull(COALESCE(renewal.member_auto_renewal_card_adw_key,'-1'),''),'|',
ifnull(COALESCE(branch1.aca_office_adw_key,'-1'),''),'|',
ifnull(COALESCE(users1.employee_adw_key,'-1'),''),'|',
ifnull(payments.BATCH_PAYMENT_KY,''),'|',
ifnull(payments.BATCH_NAME,''),'|',
ifnull(payments.EXP_CT,''),'|',
ifnull(payments.EXP_AT,''),'|',
ifnull(payments.CREATE_DT,''),'|',
ifnull(payments.STATUS,''),'|',
ifnull(payments.STATUS_DT,''),'|',
ifnull(payments.PAYMENT_AT,''),'|',
ifnull(payments.PAYMENT_METHOD_CD,''),'|',
ifnull(CODE_PEMTH.CODE_DESC,''),'|',
ifnull(payments.PAYMENT_SOURCE_CD,''),'|',
ifnull(CODE_PAYSRC.CODE_DESC,''),'|',
ifnull(payments.TRANSACTION_TYPE_CD,''),'|',
ifnull(CODE_PAYSRC_TRANSACTION.CODE_DESC,''),'|',
ifnull(payments.REASON_CD,''),'|',
ifnull(payments.PAID_BY_CD,''),'|',
ifnull(payments.ADJUSTMENT_DESCRIPTION_CD,''),'|',
ifnull(CODE_PAYADJ.CODE_DESC,''),'|',
ifnull(payments.POST_COMPLETED_FL,''),'|',
ifnull(payments.POST_DT,''),'|',
ifnull(payments.AUTO_RENEW_FL,''),'|'
))) as adw_row_hash
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_payments_received_work_source` payments
LEFT JOIN
`{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership
ON payments.MEMBERSHIP_KY=SAFE_CAST(membership.membership_source_system_key AS STRING) AND membership.active_indicator='Y'
LEFT JOIN
`{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` bill_summary
ON payments.BILL_KY=SAFE_CAST(bill_summary.bill_summary_source_key AS STRING) AND bill_summary.active_indicator='Y'
LEFT JOIN
(SELECT
branch.BRANCH_KY,
branch.BRANCH_CD,
office.membership_branch_code,
office.aca_office_adw_key,
ROW_NUMBER() OVER (PARTITION BY branch.BRANCH_KY ORDER BY NULL) AS DUPE_CHECK
FROM
`{iu.INGESTION_PROJECT}.mzp.branch` branch,
`{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` office
where office.membership_branch_code=branch.BRANCH_CD and office.active_indicator='Y'
) as branch1 
on branch1.BRANCH_KY=payments.BRANCH_KY AND branch1.DUPE_CHECK=1
LEFT JOIN
(SELECT users.USER_ID,
users.USERNAME,
emp.employee_active_directory_user_identifier,
emp.employee_adw_key,
ROW_NUMBER() OVER (PARTITION BY users.USER_ID ORDER BY NULL) AS DUPE_CHECK
FROM `{iu.INGESTION_PROJECT}.mzp.cx_iusers` users,
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` emp
where emp.employee_active_directory_user_identifier=users.USERNAME and emp.active_indicator='Y'
) users1 ON users1.USER_ID=payments.USER_ID AND users1.DUPE_CHECK=1
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYADJ'
GROUP BY CODE) CODE_PAYADJ
ON CODE_PAYADJ.CODE=payments.ADJUSTMENT_DESCRIPTION_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PEMTH'
GROUP BY CODE) CODE_PEMTH
ON CODE_PEMTH.CODE=payments.PAYMENT_METHOD_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC
ON CODE_PAYSRC.CODE=payments.PAYMENT_SOURCE_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC_TRANSACTION
ON CODE_PAYSRC_TRANSACTION.CODE=payments.TRANSACTION_TYPE_CD
LEFT OUTER JOIN
`{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` renewal
ON SAFE_CAST(renewal.member_source_arc_key AS STRING)=payments.AUTORENEWAL_CARD_KY AND renewal.active_indicator='Y'
where payments.dupe_check=1


"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_payments_received_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_type_2_hist` AS
WITH
  payments_received_hist AS (
  SELECT
    BATCH_PAYMENT_KY,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY BATCH_PAYMENT_KY ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(SAFE_CAST(last_upd_dt AS DATETIME)) OVER (PARTITION BY BATCH_PAYMENT_KY ORDER BY last_upd_dt),datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    BATCH_PAYMENT_KY,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    payments_received_hist 
  ), set_groups AS (
  SELECT
    BATCH_PAYMENT_KY,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY BATCH_PAYMENT_KY ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    BATCH_PAYMENT_KY,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    BATCH_PAYMENT_KY,
    adw_row_hash,
    grouping_column 
  )
SELECT
  BATCH_PAYMENT_KY,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped
  """

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership_payments_received
membership_payments_received_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received`
(
membership_payments_received_adw_key,
membership_adw_key,
membership_billing_summary_adw_key,
member_auto_renewal_card_adw_key,
aca_office_adw_key,
employee_adw_key,
payment_received_source_key,
payment_received_batch_key,
payment_received_name,
payment_received_count,
payment_received_expected_amount,
payment_received_created_datetime,
payment_received_status,
payment_received_status_datetime,
payment_received_amount,
payment_received_method_code,
payment_received_method_description,
payment_source_code,
payment_source_description,
payment_type_code,
payment_type_description,
payment_reason_code,
payment_paid_by_description,
payment_adjustment_code,
payment_adjustment_description,
payment_received_post_flag,
payment_received_post_datetime,
payment_received_auto_renewal_flag,
effective_start_datetime,
effective_end_datetime,
actrive_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
SELECT
membership_payments_received_adw_key,
source.membership_adw_key,
source.membership_billing_summary_adw_key,
source.member_auto_renewal_card_adw_key,
source.aca_office_adw_key,
source.employee_adw_key,
SAFE_CAST(source.BATCH_PAYMENT_KY AS INT64),
SAFE_CAST(source.BATCH_KY AS INT64),
source.BATCH_NAME,
SAFE_CAST(source.EXP_CT AS INT64),
SAFE_CAST(source.EXP_AT AS NUMERIC),
SAFE_CAST(source.CREATE_DT AS DATETIME),
source.STATUS,
SAFE_CAST(source.STATUS_DT AS DATETIME),
SAFE_CAST(source.PAYMENT_AT AS NUMERIC),
source.PAYMENT_METHOD_CD,
source.payment_received_method_description,
source.PAYMENT_SOURCE_CD,
source.payment_source_description,
source.TRANSACTION_TYPE_CD,
source.payment_type_description,
source.REASON_CD,
source.PAID_BY_CD,
source.ADJUSTMENT_DESCRIPTION_CD,
source.payment_adjustment_description,
source.POST_COMPLETED_FL,
SAFE_CAST(source.POST_DT AS DATETIME),
source.AUTO_RENEW_FL,
CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
    ( CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END) ,
    source.adw_row_hash,
    CURRENT_DATETIME() ,
    1 ,
    CURRENT_DATETIME() ,
    1 
FROM
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` source
  ON (
  source.BATCH_PAYMENT_KY=hist_type_2.BATCH_PAYMENT_KY
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_payments_received_adw_key,
    BATCH_PAYMENT_KY
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed`
  GROUP BY
    BATCH_PAYMENT_KY) pk
ON (source.BATCH_PAYMENT_KY=pk.BATCH_PAYMENT_KY)
"""

######################################################################
# Load Membership_fee Historical set of Data
######################################################################
# Queries STARTED

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

membership_fee_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_source` AS
SELECT
membership_fee_source.MEMBERSHIP_FEES_KY,
membership_fee_source.membership_ky,
membership_fee_source.status,
membership_fee_source.fee_type,
membership_fee_source.fee_dt,
membership_fee_source.waived_dt,
membership_fee_source.waived_by,
membership_fee_source.donor_nr,
membership_fee_source.waived_reason_cd,
membership_fee_source.last_upd_dt, 
  ROW_NUMBER() OVER(PARTITION BY membership_fee_source.MEMBERSHIP_FEES_KY,membership_fee_source.last_upd_dt  ORDER BY null ) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.membership_fees` AS membership_fee_source
"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_fee_work_transformed = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` AS
SELECT
  COALESCE(dim_member.member_adw_key,
    "-1") AS member_adw_key,
  COALESCE(product1.product_adw_key,
    "-1") AS product_adw_key,
  SAFE_CAST(COALESCE(membership_fee_source.MEMBERSHIP_FEES_KY,
    "-1")as int64) AS membership_fee_source_key,
  membership_fee_source.status AS status,
  membership_fee_source.fee_type AS fee_type,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.fee_dt,1,10)) AS Date) AS fee_date,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.waived_dt,1,10)) AS Date) AS waived_date,
  SAFE_CAST(membership_fee_source.waived_by as INT64)  AS waived_by,
  membership_fee_source.donor_nr AS donor_number,
  membership_fee_source.waived_reason_cd AS waived_reason_code,
  membership_fee_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(dim_member.member_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(product1.product_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(membership_fee_source.MEMBERSHIP_FEES_KY,
            "-1"),
          ''), '|', ifnull(membership_fee_source.status,
          ''), '|', ifnull(membership_fee_source.fee_type,
          ''), '|',ifnull(membership_fee_source.fee_dt,
          ''), '|',ifnull(membership_fee_source.waived_dt,
          ''), '|',ifnull(membership_fee_source.waived_by,
          ''), '|',ifnull(membership_fee_source.donor_nr,
          ''), '|',ifnull(membership_fee_source.waived_reason_cd,
          '') ))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_source` AS membership_fee_source
LEFT OUTER JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_member` dim_member
ON
  membership_fee_source.MEMBERSHIP_FEES_KY=CAST(dim_member.member_source_system_key AS string)
  AND dim_member.active_indicator='Y'
LEFT OUTER JOIN (
  SELECT
    membership.membership_ky,
    product_adw_key,
    product_sku_key,
    active_indicator,
    ROW_NUMBER() OVER(PARTITION BY membership.membership_ky ORDER BY NULL ) AS dupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_product` product,
    `{iu.INGESTION_PROJECT}.mzp.membership` membership
  WHERE
    membership.coverage_level_cd = product.product_sku_key
    AND product.active_indicator='Y' ) product1
ON
  membership_fee_source.membership_ky=product1.membership_ky
  AND product1.dupe_check=1
WHERE
  membership_fee_source.dupe_check=1
"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_fee_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_type_2_hist` AS
WITH
  membership_fee_hist AS (
  SELECT
    membership_fee_source_key ,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY membership_fee_source_key ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(cast(effective_start_datetime as datetime)) OVER (PARTITION BY membership_fee_source_key ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    membership_fee_source_key ,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    membership_fee_hist 
  ), set_groups AS (
  SELECT
    membership_fee_source_key ,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY membership_fee_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    membership_fee_source_key ,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    membership_fee_source_key ,
    adw_row_hash,
    grouping_column 
  )
SELECT
  membership_fee_source_key ,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership_fee
membership_fee_insert = f"""
INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee`
  (
membership_fee_adw_key,	
member_adw_key ,	
product_adw_key ,	
membership_fee_source_key ,	
status ,	
fee_type,
fee_date,
waived_date,
waived_by,
donor_number,
waived_reason_code,
effective_start_datetime,	
effective_end_datetime,	
active_indicator,
adw_row_hash ,
integrate_insert_datetime,	
integrate_insert_batch_number,	
integrate_update_datetime,	
integrate_update_batch_number
)
SELECT
membership_fee_adw_key ,	
source.member_adw_key ,	
source.product_adw_key ,	
source.membership_fee_source_key,	
source.status ,	
source.fee_type,
source.fee_date,
source.waived_date,
source.waived_by ,
source.donor_number,
source.waived_reason_code,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` source ON (
  source.membership_fee_source_key = hist_type_2.membership_fee_source_key
  AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_fee_adw_key ,
    membership_fee_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed`
  GROUP BY
    membership_fee_source_key ) pk
ON (source.membership_fee_source_key =pk.membership_fee_source_key)

"""

######################################################################
# Load Product Historical set of Data
######################################################################

membership_solicitation_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
 CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_solicitation_work_source` AS
  SELECT
  solicitation_source.solicitation_ky,
  solicitation_source.solicitation_cd,
  solicitation_source.group_cd,
  solicitation_source.membership_type_cd,
  solicitation_source.campaign_cd,
  solicitation_source.solicitation_category_cd,
  solicitation_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY solicitation_source.solicitation_ky ORDER BY solicitation_source.last_upd_dt DESC) AS dupe_check
  FROM `{iu.INGESTION_PROJECT}.mzp.solicitation` solicitation_source

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_solicitation_work_transformed = f"""

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` AS
SELECT
solicitation.solicitation_ky,
  solicitation.solicitation_cd,
  solicitation.group_cd,
  solicitation.membership_type_cd,
  solicitation.campaign_cd,
  solicitation.solicitation_category_cd,
  discount.discount_cd,
  solicitation.last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(solicitation.solicitation_cd,''),'|',
ifnull(solicitation.group_cd,''),'|',
ifnull(solicitation.membership_type_cd,''),'|',
ifnull(solicitation.campaign_cd,''),'|',
ifnull(solicitation.solicitation_category_cd,''),'|',
ifnull(discount.discount_cd,''),'|'
))) as adw_row_hash
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_solicitation_work_source` solicitation
LEFT JOIN
(SELECT 
      solicitation_ky,
      discount_cd,
      ROW_NUMBER() OVER(PARTITION BY solicitation_ky ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
    FROM `{iu.INGESTION_PROJECT}.mzp.solicitation_discount`
) discount
on discount.solicitation_ky=solicitation.solicitation_ky and discount.dupe_check=1
where solicitation.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_solicitation_work_type2_logic = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_type_2_hist` AS
WITH
  solicitation_hist AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY solicitation_ky ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(SAFE_CAST(last_upd_dt AS DATETIME)) OVER (PARTITION BY solicitation_ky ORDER BY last_upd_dt),datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    solicitation_hist 
  ), set_groups AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY solicitation_ky ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    solicitation_ky,
    adw_row_hash,
    grouping_column 
  )
SELECT
  solicitation_ky,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership_solicitation
membership_solicitation_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation`
(
membership_solicitation_adw_key,
membership_solicitation_source_system_key,
solicitation_code,
solicitation_group_code,
member_type_code,
solicitation_campaign_code,
solicitation_category_code,
solicitation_discount_code,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
(
SELECT
membership_solicitation_adw_key,
SAFE_CAST(source.solicitation_ky AS INT64),
source.solicitation_cd,
  source.group_cd,
  source.membership_type_cd,
  source.campaign_cd,
  source.solicitation_category_cd,
  source.discount_cd,
 CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
    ( CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END) ,
    source.adw_row_hash,
    CURRENT_DATETIME() ,
    1 ,
    CURRENT_DATETIME() ,
    1 
    FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` source
  ON (
  source.solicitation_ky=hist_type_2.solicitation_ky
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_solicitation_adw_key,
    solicitation_ky
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed`
  GROUP BY
    solicitation_ky) pk
ON (source.solicitation_ky=pk.solicitation_ky)
  )

"""

######################################################################
# Load Payment Plan Historical set of Data
######################################################################

paymentplan_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_source`  as 
SELECT 
 PLAN_BILLING_KY,
 RIDER_KY,
 source.PAYMENT_PLAN_KY,
 PAYMENT_NUMBER,
 MEMBERSHIP_PAYMENT_KY,
 PAYMENT_STATUS,
 CHARGE_DT,
 CHARGE_CT,
 PAYMENT_AT,
 source.LAST_UPD_DT,
 COST_EFFECTIVE_DT,
 MEMBERSHIP_FEES_KY,
 DONATION_HISTORY_KY,
 MEMBERSHIP_KY,
 pp.PLAN_NAME,
 pp.NUMBER_OF_PAYMENTS,
 pp.PLAN_LENGTH,
 ROW_NUMBER() OVER(PARTITION BY source.plan_billing_ky ORDER BY source.last_upd_dt DESC) AS dupe_check
FROM
`{iu.INGESTION_PROJECT}.mzp.plan_billing` as source
left join `{iu.INGESTION_PROJECT}.mzp.payment_plan` as pp on source.PAYMENT_PLAN_KY = pp.PAYMENT_PLAN_KY
"""

paymentplan_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` as 
SELECT 
 coalesce(adw_memship.membership_adw_key,'-1') as membership_adw_key,
 coalesce(adw_rider.member_rider_adw_key,'-1') as member_rider_adw_key,
 cast(PLAN_BILLING_KY as int64) as payment_plan_source_key ,
 source.PLAN_NAME as payment_plan_name,
 cast(source.NUMBER_OF_PAYMENTS as int64) as payment_plan_payment_count,
 cast(source.PLAN_LENGTH as int64) as payment_plan_months,
 cast(source.PAYMENT_NUMBER as int64) as paymentnumber,
 source.PAYMENT_STATUS as payment_plan_status,
 cast(cast(source.CHARGE_DT as datetime) as date) as payment_plan_charge_date,
 case when source.DONATION_HISTORY_KY is null then 'N' else 'Y' end as safety_fund_donation_indicator,
 CAST(source.last_upd_dt AS datetime) AS last_upd_dt,
 CAST('9999-12-31' AS datetime) effective_end_datetime,
 'Y' as active_indicator,
 TO_BASE64(MD5(CONCAT(
            ifnull(source.PLAN_BILLING_KY,''),'|',
            ifnull(source.membership_ky,''),'|',
            ifnull(source.rider_ky,''),'|',
            ifnull(source.PLAN_NAME,''),'|',
            ifnull(source.NUMBER_OF_PAYMENTS,''),'|',
            ifnull(source.PLAN_LENGTH,''),'|',
            ifnull(source.PAYMENT_NUMBER,''),'|',
            ifnull(source.PAYMENT_STATUS,''),'|',
            ifnull(source.CHARGE_DT,''),'|',
            ifnull(source.DONATION_HISTORY_KY,'')
            ))) as adw_row_hash,
 CURRENT_DATETIME() as integrate_insert_datetime,
 1 as integrate_insert_batch_number,
 CURRENT_DATETIME() as integrate_update_datetime,
 1 as integrate_update_batch_number
FROM
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_source` as source 
 LEFT JOIN `{int_u.INTEGRATION_PROJECT}.member.dim_membership` adw_memship
 on source.membership_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING) AND adw_memship.active_indicator='Y'
 LEFT JOIN `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider` adw_rider
 on source.rider_ky =SAFE_CAST(adw_rider.member_rider_source_key AS STRING) AND adw_rider.active_indicator='Y'
WHERE source.dupe_check=1 
"""

paymentplan_type2_logic = f"""

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_type_2_hist` AS
WITH
  paymentplan_hist AS (
  SELECT
    payment_plan_source_key,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY payment_plan_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY payment_plan_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` 
  ), set_grouping_column AS (
  SELECT
    payment_plan_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    paymentplan_hist 
  ), set_groups AS (
  SELECT
    payment_plan_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY payment_plan_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    payment_plan_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    payment_plan_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  payment_plan_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""
paymentplan_insert = f"""
INSERT INTO `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` 
  ( membership_payment_plan_adw_key ,    
  membership_adw_key,
  member_rider_adw_key,
  payment_plan_source_key,    
  payment_plan_name,
  payment_plan_payment_count,
  payment_plan_months,
  paymentnumber,
  payment_plan_status,
  payment_plan_charge_date,
  safety_fund_donation_indicator,
  effective_start_datetime,
  effective_end_datetime,
  active_indicator,
  adw_row_hash,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number
  )
SELECT
  membership_payment_plan_adw_key ,    
  source.membership_adw_key,
  source.member_rider_adw_key,
  source.payment_plan_source_key,   
  source.payment_plan_name, 
  source.payment_plan_payment_count,
  source.payment_plan_months,
  source.paymentnumber,
  source.payment_plan_status,
  source.payment_plan_charge_date,
  source.safety_fund_donation_indicator,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` source ON (
  source.payment_plan_source_key=hist_type_2.payment_plan_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_payment_plan_adw_key,
    payment_plan_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed`
  GROUP BY
    payment_plan_source_key) pk
ON (source.payment_plan_source_key=pk.payment_plan_source_key)

"""



####################################################################################################################################
# Membership_Payment_Applied_Detail
####################################################################################################################################
######################################################################
# Load Membership Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################


######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


member_pay_app_detail_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_source` AS
  SELECT
    membership_source.member_ky,
	membership_source.membership_fees_ky,
	membership_source.rider_ky,
	membership_source.membership_payment_ky,
	membership_source.payment_detail_ky,
	membership_source.membership_payment_at,
	membership_source.unapplied_used_at,
	membership_source.discount_history_ky,
	membership_source.last_upd_dt,
    ROW_NUMBER() OVER(PARTITION BY membership_source.payment_detail_ky, membership_source.last_upd_dt ORDER BY  NULL DESC) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.payment_detail` AS membership_source

"""

######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################


member_pay_app_detail_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_transformed` AS
 SELECT
    COALESCE(member.member_adw_key,'-1') AS member_adw_key,
    COALESCE(sub_query.product_adw_key,'-1') AS product_adw_key,
	COALESCE(summary.membership_payment_applied_summary_adw_key,'-1')	 As membership_payment_applied_summary_adw_key,
	COALESCE(membership.payment_detail_ky,'-1') As payment_applied_detail_source_key,
	summary.payment_applied_date As payment_applied_date,
	summary.payment_method_code As payment_method_code,
	summary.payment_method_description As payment_method_description,
	membership.membership_payment_at As payment_detail_amount,
	membership.unapplied_used_at As payment_applied_unapplied_amount,
	disc_hist.amount As discount_amount,
	disc_hist.counted_fl As payment_applied_discount_counted,
	disc_hist.disc_effective_dt	 As discount_effective_date,
	disc_hist.cost_effective_dt As rider_cost_effective_datetime,
	CAST (membership.last_upd_dt AS datetime) As last_upd_dt,
	TO_BASE64(MD5(CONCAT(ifnull(COALESCE(member.member_adw_key,'-1'),
            ''),'|',ifnull(COALESCE(sub_query.product_adw_key,'-1'),
            ''),'|',ifnull(COALESCE(summary.membership_payment_applied_summary_adw_key,'-1'),
            ''),'|',ifnull(COALESCE(membership.payment_detail_ky,'-1'),
            ''),'|',ifnull(safe_cast(summary.payment_applied_date as string),
            ''),'|',ifnull(summary.payment_method_code,
            ''),'|',ifnull(summary.payment_method_description,
            ''),'|',ifnull(membership.membership_payment_at,
            ''),'|',ifnull(membership.unapplied_used_at,
            ''),'|',ifnull(disc_hist.amount,
            ''),'|',ifnull(disc_hist.counted_fl,
            ''),'|',ifnull(disc_hist.disc_effective_dt,
            ''),'|',ifnull(disc_hist.cost_effective_dt,
            '')))) AS adw_row_hash
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_source` AS membership
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` AS member
  ON
    membership.member_ky =  safe_cast(member.member_source_system_key as string)
	AND member.active_indicator = 'Y'

  LEFT JOIN	
	(
		SELECT rider_ky as rider_ky, 
			rider_comp_cd as rider_comp_cd ,
			ROW_NUMBER() OVER(PARTITION BY rider_ky ORDER BY last_upd_dt DESC) AS dupe_check
		FROM  
			`{iu.INGESTION_PROJECT}.mzp.rider`  
	) AS rider
  ON 
	membership.rider_ky= rider.rider_ky
	AND rider.dupe_check=1
  LEFT JOIN
	(
		SELECT 	product.product_adw_key,
				product_sku_key
		FROM 
			`{int_u.INTEGRATION_PROJECT}.adw.dim_product` AS product
		LEFT JOIN 
			`{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` AS product_category
		ON 
			product.product_category_adw_key=product_category.product_category_adw_key
			AND product_category.active_indicator = 'Y'
			AND product.active_indicator = 'Y'
		WHERE product_category_code='RCC'
	) sub_query
  ON 
	rider.rider_comp_cd	= sub_query.product_sku_key
  LEFT JOIN
    (
		SELECT 	payment_applied_date,
				payment_method_code,
				payment_method_description ,
				membership_payment_applied_summary_adw_key,
				payment_applied_source_key
		FROM   
			`{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` 
		WHERE active_indicator = 'Y'
		GROUP BY 	payment_applied_date,
					payment_method_code,
					payment_method_description ,
					membership_payment_applied_summary_adw_key,
					payment_applied_source_key
	) AS summary
  ON
    membership.membership_payment_ky =  safe_cast(summary.payment_applied_source_key as string)
  LEFT JOIN 
    `{iu.INGESTION_PROJECT}.mzp.discount_history` AS disc_hist
  ON
    membership.discount_history_ky = disc_hist.discount_history_ky 
WHERE 
	membership.dupe_check=1

"""

######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################


member_pay_app_detail_work_type2_logic = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_type_2_hist` AS
WITH
  membership_hist AS (
  SELECT
    payment_applied_detail_source_key,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY payment_applied_detail_source_key ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY payment_applied_detail_source_key ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    payment_applied_detail_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    membership_hist 
  ), set_groups AS (
  SELECT
    payment_applied_detail_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY payment_applied_detail_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    payment_applied_detail_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    payment_applied_detail_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  payment_applied_detail_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_payment_applied_detail

membership_pay_app_detail_insert = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

INSERT INTO
  `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_detail` 
  ( membership_payment_applied_detail_adw_key,	
	member_adw_key,	
	membership_payment_applied_summary_adw_key,	
	product_adw_key,	
	payment_applied_detail_source_key,	
	payment_applied_date,	
	payment_method_code,	
	payment_method_description,	
	payment_detail_amount,	
	payment_applied_unapplied_amount,	
	discount_amount,	
	payment_applied_discount_counted,	
	discount_effective_date,	
	rider_cost_effective_datetime,	
	effective_start_datetime,	
	effective_end_datetime,	
	active_indicator,	
	adw_row_hash,	
	integrate_insert_datetime,	
	integrate_insert_batch_number,	
	integrate_update_datetime,	
	integrate_update_batch_number )
SELECT
  membership_payment_applied_detail_adw_key,
  source.member_adw_key,
  CAST(source.membership_payment_applied_summary_adw_key AS string),
  CAST (source.product_adw_key AS string),	  
  CAST(source.payment_applied_detail_source_key AS int64),
  source.payment_applied_date,
  CAST(source.payment_method_code AS string),
  source.payment_method_description,
  CAST(source.payment_detail_amount AS numeric),
  CAST(source.payment_applied_unapplied_amount AS numeric),
  CAST(source.discount_amount AS numeric),
  CAST(source.payment_applied_discount_counted AS string),
  CAST(substr(source.discount_effective_date, 0, 10) AS date),
  CAST(source.rider_cost_effective_datetime AS datetime),
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  1,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_type_2_hist` hist_type_2
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_transformed` source ON (
  source.payment_applied_detail_source_key=hist_type_2.payment_applied_detail_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_payment_applied_detail_adw_key,
    payment_applied_detail_source_key
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_transformed`
  GROUP BY
    payment_applied_detail_source_key) pk
ON (source.payment_applied_detail_source_key=pk.payment_applied_detail_source_key);

"""

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS membership_gl_payments_applied STARTED (SAHIL)

    # membership_gl_payments_applied

    # membership_gl_payments_applied - Workarea loads
    task_membership_gl_payments_applied_work_source = int_u.run_query("build_membership_gl_payments_applied_work_source", membership_gl_payments_applied_work_source)
    task_membership_gl_payments_applied_work_transformed = int_u.run_query("build_membership_gl_payments_applied_work_transformed", membership_gl_payments_applied_work_transformed)
    task_membership_gl_payments_applied_work_type2_logic = int_u.run_query("build_membership_gl_payments_applied_work_type2_logic", membership_gl_payments_applied_work_type2_logic)

    # membership_gl_payments_applied - Merge Load
    task_membership_gl_payments_applied_insert = int_u.run_query('merge_membership_gl_payments_applied',membership_gl_payments_applied_insert)

    # TASKS membership_gl_payments_applied ended (SAHIL)

    ######################################################################
    # TASKS

    # Product
    task_product_work_source = int_u.run_query('product_source', product_source)
    task_product_transformed = int_u.run_query('product_transformed', product_transformed)
    task_product_work_type2_logic = int_u.run_query('product_work_type2_logic', product_work_type2_logic)
    task_product_insert = int_u.run_query('product_insert', product_insert)

    # Membership

    # Membership - Workarea loads
    task_membership_work_source = int_u.run_query("build_membership_work_source", membership_work_source)
    task_membership_work_transformed = int_u.run_query("build_membership_work_transformed", membership_work_transformed)
    task_membership_work_type2_logic = int_u.run_query("build_membership_work_type2_logic", membership_work_type2_logic)

    # membership - Merge Load
    task_membership_insert = int_u.run_query('merge_membership', membership_insert)

    # Member

    # Member - Workarea loads
    task_member_work_source = int_u.run_query("build_member_work_source", member_work_source)
    task_member_work_transformed = int_u.run_query("build_member_work_transformed", member_work_transformed)
    task_member_work_type2_logic = int_u.run_query("build_member_work_type2_logic", member_work_type2_logic)

    # member - Merge Load
    task_member_insert = int_u.run_query('merge_member', member_insert)

    # Member_rider

    # Member_rider - Workarea loads
    task_member_rider_work_source = int_u.run_query("build_member_rider_work_source", member_rider_work_source)
    task_member_rider_work_transformed = int_u.run_query("build_member_rider_work_transformed", member_rider_work_transformed)
    task_member_rider_work_type2_logic = int_u.run_query("build_member_rider_work_type2_logic", member_rider_work_type2_logic)
    # member_rider - Insert
    task_member_rider_insert = int_u.run_query('insert_member_rider', member_rider_insert)

    # employee_role - Workarea loads
    task_employee_role_work_source = int_u.run_query("build_employee_role_work_source", employee_role_work_source)
    task_employee_role_work_transformed = int_u.run_query("build_employee_role_work_transformed", employee_role_work_transformed)
    task_employee_role_work_type2_logic = int_u.run_query("build_employee_role_work_type2_logic", employee_role_work_type2_logic)

    # employee_role - Merge Load
    task_employee_role_insert = int_u.run_query('employee_role_insert', employee_role_insert)

    # segment

    task_membership_segment_source = int_u.run_query('membership_segment_source', membership_segment_source)
    task_membership_segment_transformed = int_u.run_query('membership_segment_transformed', membership_segment_transformed)
    task_membership_segment_work_type2_logic = int_u.run_query('membership_segment_work_type2_logic', membership_segment_work_type2_logic)
    task_membership_segment_insert = int_u.run_query('membership_segment_insert', membership_segment_insert)

    # member_auto_renewal_card_
    task_member_auto_renewal_card_work_source = int_u.run_query('member_auto_renewal_card_work_source', member_auto_renewal_card_work_source)
    task_member_auto_renewal_card_work_transformed = int_u.run_query('member_auto_renewal_card_work_transformed', member_auto_renewal_card_work_transformed)
    task_member_auto_renewal_card_work_type2_logic = int_u.run_query('member_auto_renewal_card_work_type2_logic', member_auto_renewal_card_work_type2_logic)
    task_member_auto_renewal_card_insert = int_u.run_query('member_auto_renewal_card_insert', member_auto_renewal_card_insert)

    # membership_billing_summary - Workarea loads
    task_membership_billing_summary_work_source = int_u.run_query("build_membership_billing_summary_work_source", membership_billing_summary_work_source)
    task_membership_billing_summary_work_transformed = int_u.run_query("build_membership_billing_summary_work_transformed", membership_billing_summary_work_transformed)
    task_membership_billing_summary_work_type2_logic = int_u.run_query("build_membership_billing_summary_work_type2_logic", membership_billing_summary_work_type2_logic)

    # membership_billing_summary - Merge Load
    task_membership_billing_summary_insert = int_u.run_query('merge_membership_billing_summary', membership_billing_summary_insert)

    # membership_billing_detail

    # membership_billing_detail - Workarea loads
    task_membership_billing_detail_work_source = int_u.run_query("build_membership_billing_detail_work_source", membership_billing_detail_work_source)
    task_membership_billing_detail_work_transformed = int_u.run_query("build_membership_billing_detail_work_transformed", membership_billing_detail_work_transformed)
    task_membership_billing_detail_work_type2_logic = int_u.run_query("build_membership_billing_detail_work_type2_logic", membership_billing_detail_work_type2_logic)

    # membership_billing_detail - Merge Load
    task_membership_billing_detail_insert = int_u.run_query('merge_membership_billing_detail', membership_billing_detail_insert)

    # payment summary

    task_payment_applied_summary_hist_source = int_u.run_query('payment_applied_summary_hist_source', payment_applied_summary_hist_source)
    task_payment_applied_summary_hist_transformed = int_u.run_query('payment_applied_summary_hist_transformed', payment_applied_summary_hist_transformed)
    task_payment_applied_summary_work_type2_logic = int_u.run_query('payment_applied_summary_work_type2_logic', payment_applied_summary_work_type2_logic)
    task_payment_applied_summary_insert = int_u.run_query('payment_applied_summary_insert', payment_applied_summary_insert)

    # membership_payments_received - history load(Prerna)

    task_membership_payments_received_work_source = int_u.run_query("Membership_payments_received_work_source", membership_payments_received_work_source)
    task_membership_payments_received_work_transformed = int_u.run_query("Membership_payments_received_work_transformed", membership_payments_received_work_transformed)
    task_membership_payments_received_work_type2_logic = int_u.run_query("Membership_payments_received_work_type2_logic", membership_payments_received_work_type2_logic)

    task_membership_payments_received_insert = int_u.run_query('Membership_payments_received_insert', membership_payments_received_insert)

    # Membership_fee (SAHIL)

    # Membership_fee - Workarea loads
    task_membership_fee_work_source = int_u.run_query("build_membership_fee_work_source", membership_fee_work_source)
    task_membership_fee_work_transformed = int_u.run_query("build_membership_fee_work_transformed", membership_fee_work_transformed)
    task_membership_fee_work_type2_logic = int_u.run_query("build_membership_fee_work_type2_logic", membership_fee_work_type2_logic)
    # Membership_fee - Merge Load
    task_membership_fee_insert = int_u.run_query('merge_membership_fee', membership_fee_insert)

    # membership solicitation tasks (Prerna)
    task_membership_solicitation_work_source = int_u.run_query("build_membership_solicitation_work_source", membership_solicitation_work_source)
    task_membership_solicitation_work_transformed = int_u.run_query("build_membership_solicitation_work_transformed", membership_solicitation_work_transformed)
    task_membership_solicitation_work_type2_logic = int_u.run_query("build_membership_solicitation_work_type2_logic", membership_solicitation_work_type2_logic)
    # Membership_solicitation - Merge Load
    task_membership_solicitation_insert = int_u.run_query('merge_membership_solicitation', membership_solicitation_insert)

    # Payment Plan
    task_payment_plan_source = int_u.run_query('paymentplan_source', paymentplan_source)
    task_payment_plan_transformed = int_u.run_query('paymentplan_transformed', paymentplan_transformed)
    task_payment_plan_type2_logic = int_u.run_query('paymentplan_type2_logic', paymentplan_type2_logic)
    task_payment_plan_insert = int_u.run_query('paymentplan_insert', paymentplan_insert)

    # membership_payment_applied_detail
    task_member_pay_app_detail_work_source = int_u.run_query("build_member_pay_app_detail_work_source",
                                                    member_pay_app_detail_work_source)
    task_member_pay_app_detail_work_transformed = int_u.run_query("build_member_pay_app_detail_work_transformed",
                                                         member_pay_app_detail_work_transformed)
    task_member_pay_app_detail_work_type2_logic = int_u.run_query("build_member_pay_app_detail_work_type2_logic",
                                                         member_pay_app_detail_work_type2_logic)
    task_membership_pay_app_detail_insert = int_u.run_query('merge_membership_payment_applied_detail',
                                               membership_pay_app_detail_insert)

    ######################################################################
    # DEPENDENCIES

    # membership solicitation tasks
    task_membership_solicitation_work_source >> task_membership_solicitation_work_transformed >> task_membership_solicitation_work_type2_logic >> task_membership_solicitation_insert >> task_membership_work_source

    # Product
    task_product_work_source >> task_product_transformed
    task_product_transformed >> task_product_work_type2_logic
    task_product_work_type2_logic >> task_product_insert

    task_product_insert >> task_membership_work_source

    # Membership

    task_membership_work_source >> task_membership_work_transformed
    task_membership_work_transformed >> task_membership_work_type2_logic

    task_membership_work_type2_logic >> task_membership_insert

    # Member
    task_membership_insert >> task_member_work_source

    task_member_work_source >> task_member_work_transformed
    task_member_work_transformed >> task_member_work_type2_logic

    task_member_work_type2_logic >> task_member_insert

    # Member_rider
    task_member_insert >> task_member_rider_work_source

    task_member_rider_work_source >> task_member_rider_work_transformed
    task_member_rider_work_transformed >> task_member_rider_work_type2_logic

    task_member_rider_work_type2_logic >> task_member_rider_insert

    # employee role
    task_member_insert >> task_employee_role_work_source >> task_employee_role_work_transformed >> task_employee_role_work_type2_logic >> task_employee_role_insert

    # segment
    task_membership_segment_source >> task_membership_segment_transformed
    task_membership_segment_transformed >> task_membership_segment_work_type2_logic
    task_membership_segment_work_type2_logic >> task_membership_segment_insert

    task_membership_insert >> task_membership_segment_source

    # membership_billing_summary
    task_member_rider_insert >> task_membership_billing_summary_work_source >> task_membership_billing_summary_work_transformed >> task_membership_billing_summary_work_type2_logic >> task_membership_billing_summary_insert

    # membership_billing_detail
    task_membership_billing_summary_insert >> task_membership_billing_detail_work_source >> task_membership_billing_detail_work_transformed >> task_membership_billing_detail_work_type2_logic >> task_membership_billing_detail_insert

    # payment summary
    task_membership_insert >> task_payment_applied_summary_hist_source >> task_payment_applied_summary_hist_transformed >> task_payment_applied_summary_work_type2_logic >> task_payment_applied_summary_insert

    [task_membership_billing_summary_insert,task_membership_insert] >> task_membership_payments_received_work_source >> task_membership_payments_received_work_transformed >> task_membership_payments_received_work_type2_logic >> task_membership_payments_received_insert

    # Membership_fee
    task_member_insert >> task_membership_fee_work_source >> task_membership_fee_work_transformed >> task_membership_fee_work_type2_logic >> task_membership_fee_insert

    # Payment Plan
    task_member_rider_insert >> task_payment_plan_source >> task_payment_plan_transformed >> task_payment_plan_type2_logic >> task_payment_plan_insert

    # membership_gl_payments_applied
    task_membership_insert >> task_membership_gl_payments_applied_work_source >> task_membership_gl_payments_applied_work_transformed >> task_membership_gl_payments_applied_work_type2_logic >> task_membership_gl_payments_applied_insert

    # member_auto_renewal_card_
    task_member_auto_renewal_card_work_source >> task_member_auto_renewal_card_work_transformed >> task_member_auto_renewal_card_work_type2_logic >> task_member_auto_renewal_card_insert >> task_membership_insert


    # membership_payment_applied_detail
    task_payment_applied_summary_insert >> task_member_pay_app_detail_work_source >> task_member_pay_app_detail_work_transformed >> task_member_pay_app_detail_work_type2_logic >> task_membership_pay_app_detail_insert
