CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_transformed` AS
SELECT
  COALESCE(member.mbr_adw_key,
    '-1') AS mbr_adw_key,
  COALESCE(sub_query.product_adw_key,
    '-1') AS product_adw_key,
  COALESCE(summary.mbrs_payment_apd_summ_adw_key,
    '-1') AS mbrs_payment_apd_summ_adw_key,
  COALESCE(membership.payment_detail_ky,
    '-1') AS payment_apd_dtl_source_key,
  summary.payment_applied_dt AS payment_applied_dt,
  summary.payment_method_cd AS payment_method_cd,
  summary.payment_method_desc AS payment_method_desc,
  membership.membership_payment_at AS payment_detail_amt,
  membership.unapplied_used_at AS payment_applied_unapplied_amt,
  disc_hist.amount AS discount_amt,
  disc_hist.counted_fl AS pymt_apd_discount_counted,
  disc_hist.disc_effective_dt AS discount_effective_dt,
  disc_hist.cost_effective_dt AS rider_cost_effective_dtm,
  CAST (membership.last_upd_dt AS datetime) AS last_upd_dt,
  TO_BASE64(MD5(CONCAT(ifnull(COALESCE(member.mbr_adw_key,
            '-1'),
          ''),'|',ifnull(COALESCE(sub_query.product_adw_key,
            '-1'),
          ''),'|',ifnull(COALESCE(summary.mbrs_payment_apd_summ_adw_key,
            '-1'),
          ''),'|',ifnull(COALESCE(membership.payment_detail_ky,
            '-1'),
          ''),'|',ifnull(SAFE_CAST(summary.payment_applied_dt AS string),
          ''),'|',ifnull(summary.payment_method_cd,
          ''),'|',ifnull(summary.payment_method_desc,
          ''),'|',ifnull(membership.membership_payment_at,
          ''),'|',ifnull(membership.unapplied_used_at,
          ''),'|',ifnull(disc_hist.amount,
          ''),'|',ifnull(disc_hist.counted_fl,
          ''),'|',ifnull(disc_hist.disc_effective_dt,
          ''),'|',ifnull(disc_hist.cost_effective_dt,
          '')))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_source` AS membership
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` AS member
ON
  membership.member_ky = SAFE_CAST(member.mbr_source_system_key AS string)
  AND member.actv_ind = 'Y'
LEFT JOIN (
  SELECT
    rider_ky AS rider_ky,
    rider_comp_cd AS rider_comp_cd,
    ROW_NUMBER() OVER(PARTITION BY rider_ky ORDER BY last_upd_dt DESC) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.rider` ) AS rider
ON
  membership.rider_ky= rider.rider_ky
  AND rider.dupe_check=1
LEFT JOIN (
  SELECT
    product.product_adw_key,
    product_sku_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` AS product
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` AS product_category
  ON
    product.product_category_adw_key=product_category.product_category_adw_key
    AND product_category.actv_ind = 'Y'
    AND product.actv_ind = 'Y'
  WHERE
    product_category_cd='RCC' ) sub_query
ON
  rider.rider_comp_cd = sub_query.product_sku_key
LEFT JOIN (
  SELECT
    payment_applied_dt,
    payment_method_cd,
    payment_method_desc,
    mbrs_payment_apd_summ_adw_key,
    payment_applied_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`
  WHERE
    actv_ind = 'Y'
  GROUP BY
    payment_applied_dt,
    payment_method_cd,
    payment_method_desc,
    mbrs_payment_apd_summ_adw_key,
    payment_applied_source_key ) AS summary
ON
  membership.membership_payment_ky = SAFE_CAST(summary.payment_applied_source_key AS string)
LEFT JOIN (
  SELECT
    discount_history_ky,
    amount,
    counted_fl,
    disc_effective_dt,
    cost_effective_dt,
    ROW_NUMBER() OVER(PARTITION BY discount_history_ky ORDER BY LAST_UPD_DT DESC) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.discount_history` disc_hist ) AS disc_hist
ON
  (membership.discount_history_ky = disc_hist.discount_history_ky
    AND disc_hist.dupe_check = 1)
WHERE
  membership.dupe_check=1