CREATE OR REPLACE TABLE
  `adw-dev.adw_work.mzp_member_solicitation_work_source` AS
  SELECT
  solicitation_source.solicitation_ky,
  solicitation_source.solicitation_cd,
  solicitation_source.group_cd,
  solicitation_source.membership_type_cd,
  solicitation_source.campaign_cd,
  solicitation_source.solicitation_category_cd,
  solicitation_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY solicitation_source.solicitation_ky ORDER BY solicitation_source.last_upd_dt DESC) AS dupe_check
  FROM `adw-lake-dev.mzp.solicitation` solicitation_source
  
  
-------------------------------------------------------------
  
  CREATE OR REPLACE TABLE
    `adw-dev.adw_work.mzp_solicitation_work_transformed` AS
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
FROM `adw-dev.adw_work.mzp_member_solicitation_work_source` solicitation
LEFT JOIN
(SELECT 
      solicitation_ky,
      discount_cd,
      ROW_NUMBER() OVER(PARTITION BY solicitation_ky ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
    FROM `adw-lake-dev.mzp.solicitation_discount`
) discount
on discount.solicitation_ky=solicitation.solicitation_ky and discount.dupe_check=1
where solicitation.dupe_check=1

------------------------------------------------------------------

CREATE OR REPLACE TABLE
  `adw-dev.adw_work.mzp_solicitation_work_type_2_hist` AS
WITH
  solicitation_hist AS (
  SELECT
    solicitation_ky,
    adw_row_hash,
    last_upd_dt AS effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY solicitation_ky ORDER BY last_upd_dt) AS prev_row_hash,
    coalesce(LEAD(SAFE_CAST(last_upd_dt AS DATETIME)) OVER (PARTITION BY solicitation_ky ORDER BY last_upd_dt),datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `adw-dev.adw_work.mzp_solicitation_work_transformed` 
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
---------------------------------------------------------------------------------------

INSERT INTO `adw-dev.member.dim_membership_solicitation`
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
  `adw-dev.adw_work.mzp_solicitation_work_type_2_hist` hist_type_2
JOIN
  `adw-dev.adw_work.mzp_solicitation_work_transformed` source
  ON (
  source.solicitation_ky=hist_type_2.solicitation_ky
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS membership_solicitation_adw_key,
    solicitation_ky
  FROM
    `adw-dev.adw_work.mzp_solicitation_work_transformed`
  GROUP BY
    solicitation_ky) pk
ON (source.solicitation_ky=pk.solicitation_ky)
  )

