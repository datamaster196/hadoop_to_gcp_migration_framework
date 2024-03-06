CREATE OR REPLACE TABLE
`{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_stage` AS
SELECT
  COALESCE(target.emp_adw_key ,GENERATE_UUID()) as emp_adw_key,
  source.emp_typ_cd,
  source.emp_active_directory_user_id,
  source.active_directory_email_adw_key,
  source.emp_hr_id,
  SAFE_CAST(source.emp_hr_hire_dt as DATE) as emp_hr_hire_dt,
  SAFE_CAST(source.emp_hr_term_dt as DATE) as emp_hr_term_dt,
  source.emp_hr_term_reason,
  source.emp_hr_status,
  source.emp_hr_region,
  source.emp_loc_hr,
  source.emp_hr_title,
  source.emp_hr_sprvsr,
  source.fax_phone_adw_key,
  source.phone_adw_key,
  source.nm_adw_key,
  source.email_adw_key,
  source.emp_hr_job_cd,
  source.emp_hr_cost_center,
  source.emp_hr_position_typ,
  source.last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} as integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed` source
left  join
`{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` target
on (source.emp_active_directory_user_id=target.emp_active_directory_user_id
and target.actv_ind='Y')
where  target.emp_adw_key IS NULL
 OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  emp_adw_key,
  target.emp_typ_cd,
  target.emp_active_directory_user_id,
  target.active_directory_email_adw_key,
  target.emp_hr_id ,
  target.emp_hr_hire_dt,
  target.emp_hr_term_dt,
  target.emp_hr_term_reason,
  target.emp_hr_status,
  target.emp_hr_region,
  target.emp_loc_hr,
  target.emp_hr_title,
  target.emp_hr_sprvsr,
  target.fax_phone_adw_key,
  target.phone_adw_key,
  target.nm_adw_key,
  target.email_adw_key,
  target.emp_hr_job_cd,
  target.emp_hr_cost_center,
  target.emp_hr_position_typ,
  target.effective_start_datetime,
  DATETIME_SUB(source.last_upd_dt, INTERVAL 1 SECOND) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  {{ dag_run.id }} as integrate_update_batch_number
  FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` target
ON
  (source.emp_active_directory_user_id=target.emp_active_directory_user_id
    AND target.actv_ind='Y')
 WHERE  source.adw_row_hash <> target.adw_row_hash