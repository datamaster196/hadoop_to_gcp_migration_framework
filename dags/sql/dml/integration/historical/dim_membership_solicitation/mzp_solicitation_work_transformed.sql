CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_transformed` AS
SELECT
  solicitation.solicitation_ky,
  solicitation.solicitation_cd,
  solicitation.group_cd,
  solicitation.membership_type_cd,
  solicitation.campaign_cd,
  solicitation.solicitation_category_cd,
  discount.discount_cd,
  solicitation.last_upd_dt,
  TO_BASE64(MD5(CONCAT(ifnull(solicitation.solicitation_cd,
          ''),'|', ifnull(solicitation.group_cd,
          ''),'|', ifnull(solicitation.membership_type_cd,
          ''),'|', ifnull(solicitation.campaign_cd,
          ''),'|', ifnull(solicitation.solicitation_category_cd,
          ''),'|', ifnull(discount.discount_cd,
          ''),'|' ))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_solicitation_work_source` solicitation
LEFT JOIN (
  SELECT
    solicitation_ky,
    discount_cd,
    ROW_NUMBER() OVER(PARTITION BY solicitation_ky ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.solicitation_discount` ) discount
ON
  discount.solicitation_ky=solicitation.solicitation_ky
  AND discount.dupe_check=1
WHERE
  solicitation.dupe_check=1