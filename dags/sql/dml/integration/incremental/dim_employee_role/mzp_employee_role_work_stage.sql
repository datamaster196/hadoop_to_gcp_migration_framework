CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_role_work_stage` AS
SELECT
  COALESCE(target.emp_role_adw_key,
    GENERATE_UUID()) emp_role_adw_key,
  COALESCE(source.emp_adw_key,
    "-1") AS emp_adw_key,
  source.nm_adw_key AS nm_adw_key,
  COALESCE(source.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  source.emp_biz_role_line_cd AS emp_biz_role_line_cd ,
  source.emp_role_id AS emp_role_id,
  source.emp_role_typ AS emp_role_typ,
  SAFE_CAST(source.effective_start_datetime AS DATETIME) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_role_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role` target
ON
  (source.emp_role_id=target.emp_role_id
    AND target.actv_ind='Y')
WHERE
  target.emp_role_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.emp_role_adw_key,
  target.emp_adw_key,
  target.nm_adw_key,
  target.aca_office_adw_key,
  target.emp_biz_role_line_cd ,
  target.emp_role_id,
  target.emp_role_typ,
  target.effective_start_datetime,
  DATETIME_SUB(CAST(source.effective_start_datetime AS datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  target.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_role_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role` target
ON
  (source.emp_role_id=target.emp_role_id
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash