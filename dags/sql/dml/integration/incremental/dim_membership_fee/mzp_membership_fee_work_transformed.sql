CREATE OR REPLACE TABLE  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_transformed` AS
SELECT
  COALESCE(dim_member.mbr_adw_key,
    "-1") AS mbr_adw_key,
  COALESCE(product1.product_adw_key,
    "-1") AS product_adw_key,
  SAFE_CAST(COALESCE(membership_fee_source.MEMBERSHIP_FEES_KY,
    "-1")as int64) AS mbrs_fee_source_key,
  membership_fee_source.status AS status,
  membership_fee_source.fee_type AS fee_typ,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.fee_dt,1,10)) AS Date) AS fee_dt,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.waived_dt,1,10)) AS Date) AS waived_dt,
  SAFE_CAST(membership_fee_source.waived_by as INT64)  AS waived_by,
  membership_fee_source.donor_nr AS donor_nbr,
  membership_fee_source.waived_reason_cd AS waived_reason_cd,
  membership_fee_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(dim_member.mbr_adw_key,
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_source` AS membership_fee_source
LEFT OUTER JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` dim_member
ON
  safe_cast(membership_fee_source.MEMBER_KY as INT64) =dim_member.mbr_source_system_key
  AND dim_member.actv_ind='Y'
LEFT OUTER JOIN (
  SELECT
    membership.membership_ky,
    product_adw_key,
    product_sku_key,
    actv_ind,
    ROW_NUMBER() OVER(PARTITION BY membership.membership_ky ORDER BY NULL ) AS dupe_check
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` product,
    `{{ var.value.INGESTION_PROJECT }}.mzp.membership` membership
  WHERE
    membership.coverage_level_cd = product.product_sku_key
    AND product.actv_ind='Y' ) product1
ON
  membership_fee_source.membership_ky=product1.membership_ky
  AND product1.dupe_check=1
WHERE
  membership_fee_source.dupe_check=1