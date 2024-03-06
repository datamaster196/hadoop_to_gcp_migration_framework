CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_detail_work_transformed` AS
SELECT
  billing_summary.mbrs_billing_summary_adw_key,
  billing_summary.mbrs_adw_key,
  adw_mem.mbr_adw_key,
  adw_product.product_adw_key,
  adw_membership_fee.mbrs_fee_adw_key,
  billing_detail_source.bill_detail_source_key,
  billing_detail_source.bill_summary_source_key,
  billing_detail_source.bill_detail_amt,
  billing_summary.bill_process_dt,
  billing_summary.bill_notice_nbr,
  billing_summary.bill_typ,
  billing_detail_source.bill_detail_text,
  billing_detail_source.rider_billing_category_cd,
  adw_cx_codes.code_desc AS rider_billing_category_desc,
  billing_detail_source.rider_solicit_cd,
  CAST (billing_detail_source.last_upd_dt AS datetime) AS last_upd_dt,
  TO_BASE64(MD5(CONCAT(ifnull(billing_summary.mbrs_adw_key,
          ''),'|', ifnull(adw_mem.mbr_adw_key,
          ''),'|', ifnull(adw_membership_fee.mbrs_fee_adw_key,
          ''),'|', ifnull(billing_detail_source.bill_detail_source_key,
          ''),'|', ifnull(billing_detail_source.bill_summary_source_key,
          ''),'|', ifnull(billing_detail_source.bill_detail_amt,
          ''),'|', ifnull(CAST(billing_summary.bill_process_dt AS STRING),
          ''),'|', ifnull(CAST(billing_summary.bill_notice_nbr AS STRING),
          ''),'|', ifnull(billing_summary.bill_typ,
          ''),'|', ifnull(billing_detail_source.bill_detail_text,
          ''),'|', ifnull(billing_detail_source.rider_billing_category_cd,
          ''),'|', ifnull(billing_detail_source.rider_solicit_cd,
          ''),'|', ifnull(adw_cx_codes.code_desc,
          ''),'|', ifnull(billing_detail_source.rider_solicit_cd,
          ''),'|' ))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_detail_work_source` AS billing_detail_source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary` AS billing_summary
ON
  billing_detail_source.bill_summary_source_key =SAFE_CAST(billing_summary.bill_summary_source_key AS STRING)
  AND actv_ind='Y'
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` adw_mem
ON
  billing_detail_source.member_ky =SAFE_CAST(adw_mem.mbr_source_system_key AS STRING)
  AND adw_mem.actv_ind='Y'
LEFT JOIN (
  SELECT
    rider_ky,
    rider_comp_cd,
    ROW_NUMBER() OVER(PARTITION BY rider_ky ORDER BY last_upd_dt DESC) AS latest_rec
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.rider`) adw_rider
ON
  billing_detail_source.rider_ky = adw_rider.rider_ky
  AND adw_rider.latest_rec = 1
LEFT JOIN (
  SELECT
    a.product_adw_key,
    a.product_sku_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` a
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` b
  ON
    a.product_category_adw_key = b.product_category_adw_key
  WHERE
    b.product_category_cd = 'RCC'
    AND a.actv_ind='Y'
    AND b.actv_ind='Y') adw_product
ON
  adw_rider.rider_comp_cd = adw_product.product_sku_key
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee` adw_membership_fee
ON
  billing_detail_source.membership_fees_ky =SAFE_CAST(adw_membership_fee.mbrs_fee_source_key AS STRING)
  AND adw_membership_fee.actv_ind='Y'
LEFT JOIN (
  SELECT
    code,
    code_desc,
    ROW_NUMBER() OVER(PARTITION BY code ORDER BY last_upd_dt DESC) AS latest_rec
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
  WHERE
    code_type = 'BILTYP') adw_cx_codes
ON
  billing_detail_source.rider_billing_category_cd =adw_cx_codes.code
  AND adw_cx_codes.latest_rec = 1
WHERE
  dupe_check = 1