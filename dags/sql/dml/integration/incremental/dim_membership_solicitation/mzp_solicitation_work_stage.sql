CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_stage` AS
SELECT
COALESCE(target.mbrs_solicit_adw_key ,GENERATE_UUID()) AS mbrs_solicit_adw_key,
SAFE_CAST(source.solicitation_ky AS INT64) AS mbrs_solicit_source_system_key ,
source.solicitation_cd AS solicit_cd ,
  source.group_cd AS solicit_group_cd ,
  source.membership_type_cd AS mbr_typ_cd ,
  source.campaign_cd AS solicit_camp_cd ,
  source.solicitation_category_cd AS solicit_category_cd ,
  source.discount_cd AS solicit_discount_cd ,
SAFE_CAST(source.last_upd_dt AS DATETIME) AS effective_start_datetime,
CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    source.adw_row_hash AS adw_row_hash ,
    CURRENT_DATETIME() integrate_insert_datetime,
    {{ dag_run.id }} integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    {{ dag_run.id }} integrate_update_batch_number
 FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_transformed` source
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` target
  ON
    (source.solicitation_ky =SAFE_CAST(target.mbrs_solicit_source_system_key AS STRING)
      AND target.actv_ind='Y')
  WHERE
    target.mbrs_solicit_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
    target.mbrs_solicit_adw_key,
target.mbrs_solicit_source_system_key,
target.solicit_cd,
target.solicit_group_cd,
target.mbr_typ_cd,
target.solicit_camp_cd,
target.solicit_category_cd,
target.solicit_discount_cd,
    target.effective_start_datetime,
    DATETIME_SUB(SAFE_CAST(source.last_upd_dt AS DATETIME),
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS actv_ind,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    target.integrate_update_batch_number
FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_transformed` source
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` target
  ON
    (source.solicitation_ky=SAFE_CAST(target.mbrs_solicit_source_system_key AS STRING)
      AND target.actv_ind='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash