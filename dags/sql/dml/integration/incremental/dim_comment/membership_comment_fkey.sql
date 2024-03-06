CREATE OR REPLACE TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_comment_fkey` AS
WITH employee_lookup AS (
  SELECT
    aa.user_id,
    bb.emp_adw_key
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.cx_iusers` aa
  LEFT JOIN (
    SELECT
      emp_adw_key,
      emp_active_directory_user_id  AS username
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee`
    WHERE
      actv_ind = 'Y' ) bb
  ON
    (aa.username = bb.username)
), membership_lookup AS (
    SELECT
      mbrs_adw_key as comment_relation_adw_key,
      CAST(mbrs_source_system_key AS String) AS mbrs_source_system_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`
    WHERE
      actv_ind = 'Y'
)
SELECT
  COALESCE(bb.emp_adw_key,'-1') AS created_emp_adw_key,
  COALESCE(cc.emp_adw_key,'-1') AS resolved_emp_adw_key,
  COALESCE(dd.comment_relation_adw_key,'-1') AS comment_relation_adw_key,
  'MZP' AS comment_source_system_nm,
  CAST(membership_comment_ky AS INT64) AS comment_source_system_key,
  SAFE_CAST(create_dt AS DATETIME) AS comment_creation_dtm,
  comments AS comment_text,
  SAFE_CAST(resolved_dt AS DATETIME) AS comment_resolved_dtm,
  resolution AS comment_resltn_text,
  SAFE_CAST(last_upd_dt AS DATETIME)  AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT(
    COALESCE(create_user_id,''),
    COALESCE(resolved_user_id,''),
    COALESCE(membership_ky,''),
    COALESCE(membership_comment_ky,''),
    COALESCE(create_dt,''),
    COALESCE(comments,''),
    COALESCE(resolved_dt,''),
    COALESCE(resolution,'')
  ))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_comment_source` aa
LEFT JOIN
   employee_lookup bb ON (aa.create_user_id = bb.user_id)
LEFT JOIN
   employee_lookup cc ON (aa.resolved_user_id = cc.user_id)
LEFT JOIN
   membership_lookup dd ON (aa.membership_ky = dd.mbrs_source_system_key)
WHERE dupe_check = 1;
