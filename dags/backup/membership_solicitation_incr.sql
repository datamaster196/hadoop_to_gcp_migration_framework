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
  WHERE
  CAST(solicitation_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `adw-dev.member.dim_membership_solicitation`)
--------------------------------------------------------------------------------

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

----------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE
    `adw-dev.adw_work.mzp_solicitation_work_stage` AS
SELECT
COALESCE(target.membership_solicitation_adw_key ,GENERATE_UUID()) AS membership_solicitation_adw_key,
SAFE_CAST(source.solicitation_ky AS INT64) AS membership_solicitation_source_system_key ,
source.solicitation_cd AS solicitation_code ,
  source.group_cd AS solicitation_group_code ,
  source.membership_type_cd AS member_type_code ,
  source.campaign_cd AS solicitation_campaign_code ,
  source.solicitation_category_cd AS solicitation_category_code ,
  source.discount_cd AS solicitation_discount_code ,
SAFE_CAST(source.last_upd_dt AS DATETIME) AS effective_start_datetime,
CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash AS adw_row_hash ,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number 
 FROM
    `adw-dev.adw_work.mzp_solicitation_work_transformed` source
  LEFT JOIN
    `adw-dev.member.dim_membership_solicitation` target
  ON
    (source.solicitation_ky =SAFE_CAST(target.membership_solicitation_source_system_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_solicitation_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
    target.membership_solicitation_adw_key,
target.membership_solicitation_source_system_key,
target.solicitation_code,
target.solicitation_group_code,
target.member_type_code,
target.solicitation_campaign_code,
target.solicitation_category_code,
target.solicitation_discount_code,
    target.effective_start_datetime,
    DATETIME_SUB(SAFE_CAST(source.last_upd_dt AS DATETIME),
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    target.integrate_update_batch_number
FROM
    `adw-dev.adw_work.mzp_solicitation_work_transformed` source
  JOIN
    `adw-dev.member.dim_membership_solicitation` target
  ON
    (source.solicitation_ky=SAFE_CAST(target.membership_solicitation_source_system_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash
	
-----------------------------------------------------------------------------------------------------------

MERGE INTO
    `adw-dev.member.dim_membership_solicitation` a
  USING
    `adw-dev.adw_work.mzp_solicitation_work_stage` b
    ON (a.membership_solicitation_adw_key = b.membership_solicitation_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
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
integrate_update_batch_number)
VALUES
(
b.membership_solicitation_adw_key,
b.membership_solicitation_source_system_key,
b.solicitation_code,
b.solicitation_group_code,
b.member_type_code,
b.solicitation_campaign_code,
b.solicitation_category_code,
b.solicitation_discount_code,
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
