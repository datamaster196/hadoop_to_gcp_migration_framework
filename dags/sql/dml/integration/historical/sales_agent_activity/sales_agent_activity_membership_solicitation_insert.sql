INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` ( mbrs_solicit_adw_key,
    mbrs_solicit_source_system_key,
    solicit_cd,
    solicit_group_cd,
    mbr_typ_cd,
    solicit_camp_cd,
    solicit_category_cd,
    solicit_discount_cd,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  GENERATE_UUID() AS mbrs_solicit_adw_key,
  -1 AS mbrs_solicit_source_system_key,
  saa_src.solicitation_cd,
  'Legacy' AS group_cd,
  '' AS membership_type_cd,
  '' AS campaign_cd,
  '' AS solicitation_category_cd,
  '' AS discount_cd,
  CAST('1900-01-01' AS datetime) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) AS effective_end_datetime,
  'Y',
  TO_BASE64(MD5(CONCAT( '-1','|',    -- mbrs_solicit_source_system_key
        ifnull(saa_src.solicitation_cd,
          ''),'|',  -- solicitation_cd
        'Legacy','|', -- group_cd
        '','|', -- membership_type_cd
        '','|', -- campaign_cd
        '','|', -- solicitation_category_cd
        '','|'  -- discount_cd
        ))) AS adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM (
  SELECT
    solicitation_cd
  FROM (
    SELECT
      DISTINCT solicitation_cd
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_source`
    WHERE
      dupe_check = 1 ) saa_sol
  WHERE
    saa_sol.solicitation_cd NOT IN (
    SELECT
      DISTINCT solicit_cd
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation`
    WHERE
      actv_ind = 'Y' ) ) saa_src


