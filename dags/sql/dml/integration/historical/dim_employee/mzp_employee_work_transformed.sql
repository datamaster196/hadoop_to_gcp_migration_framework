CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_work_transformed` AS
SELECT
  employee_source.emp_typ_cd,
  employee_source.username,
  coalesce(ad_email.email_adw_key,
    "-1") AS active_directory_email_adw_key,
  employee_source.emp_hr_id ,
  employee_source.emp_hr_hire_dt,
  employee_source.emp_hr_term_dt,
  employee_source.emp_hr_term_reason,
  employee_source.emp_hr_status,
  employee_source.emp_hr_region,
  employee_source.emp_loc_hr,
  employee_source.emp_hr_title,
  employee_source.emp_hr_sprvsr,
  employee_source.fax_phone_adw_key,
  employee_source.phone_adw_key,
  employee_source.nm_adw_key,
  employee_source.email_adw_key,
  employee_source.emp_hr_job_cd,
  employee_source.emp_hr_cost_center,
  employee_source.emp_hr_position_typ,
  employee_source.dupe_check,
  CAST('1900-01-01' AS datetime) AS last_upd_dt,
  TO_BASE64(MD5(CONCAT( ifnull(employee_source.emp_typ_cd,
          ''),'|',ifnull(employee_source.username,
          ''),'|',ifnull(coalesce(ad_email.email_adw_key,
            "-1"),
          ''),'|',ifnull(employee_source.emp_hr_id ,
          ''),'|',ifnull(employee_source.emp_hr_hire_dt,
          ''),'|',ifnull(employee_source.emp_hr_term_dt,
          ''),'|',ifnull(employee_source.emp_hr_term_reason,
          ''),'|',ifnull(employee_source.emp_hr_status,
          ''),'|',ifnull(employee_source.emp_hr_region,
          ''),'|',ifnull(employee_source.emp_loc_hr,
          ''),'|',ifnull(employee_source.emp_hr_title,
          ''),'|',ifnull(employee_source.emp_hr_sprvsr,
          ''),'|',ifnull(employee_source.fax_phone_adw_key,
          ''),'|',ifnull(employee_source.phone_adw_key,
          ''),'|',ifnull(employee_source.nm_adw_key,
          ''),'|',ifnull(employee_source.email_adw_key,
          ''),'|',ifnull(employee_source.emp_hr_job_cd,
          ''),'|',ifnull(employee_source.emp_hr_cost_center,
          ''),'|',ifnull(employee_source.emp_hr_position_typ,
          '')))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_employee_source` employee_source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` ad_email
ON
  (employee_source.email=ad_email.raw_email_nm)
WHERE
  employee_source.dupe_check =1