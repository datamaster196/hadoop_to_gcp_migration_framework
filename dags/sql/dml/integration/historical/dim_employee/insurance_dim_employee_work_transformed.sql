CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed` as
SELECT
  emp_typ_cd,
  loginid as emp_active_directory_user_id ,
  flex_id as emp_hr_id ,
  dateoriginalhire as emp_hr_hire_dt ,
  terminationdate as emp_hr_term_dt ,
  eetermwhy as emp_hr_term_reason ,
  status as emp_hr_status ,
  region as emp_hr_region ,
  location as emp_loc_hr ,
  title as emp_hr_title ,
  supervisor as emp_hr_sprvsr ,
  ejjobcode as emp_hr_job_cd ,
  wd_costcenter as emp_hr_cost_center ,
  position_time_type as emp_hr_position_typ,
  last_upd_dt,
  active_dir_email,
  email,
  workphone,
  faxphone,
  firstname,
  lastname,
  active_directory_email_adw_key,
  email_adw_key,
  phone_adw_key,
  fax_phone_adw_key,
  nm_adw_key,
  TO_BASE64(MD5(CONCAT(ifnull(COALESCE(emp_typ_cd,
            '-1'),
          ''),'|',ifnull(COALESCE(loginid,
            '-1'),
          ''),'|',ifnull(flex_id,
          ''),'|',ifnull(SAFE_CAST(dateoriginalhire AS string),
          ''),'|',ifnull(SAFE_CAST(terminationdate AS string),
          ''),'|',ifnull(eetermwhy,
          ''),'|',ifnull(status,
          ''),'|',ifnull(COALESCE(region,
            '-1'),
          ''),'|',ifnull(location,
          ''),'|',ifnull(title,
          ''),'|',ifnull(COALESCE(supervisor,
            '-1'),
          ''),'|',ifnull(ejjobcode,
          ''),'|',ifnull(COALESCE(wd_costcenter,
            '-1'),
          ''),'|',ifnull(position_time_type,
          ''),ifnull(active_directory_email_adw_key,
          ''),ifnull(email_adw_key,
          ''),ifnull(phone_adw_key,
          ''),ifnull(fax_phone_adw_key,
          ''),ifnull(nm_adw_key,
          ''
          )))) AS adw_row_hash
FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_source` as source
WHERE
    dupe_check=1;
    
--------------------------------------------------------------------------------------------------------------------------------------------
------ update statement for effective start dt with CURRENT_DATETIME() for those records which are already existing in dim_employee---------

update `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed` a
set last_upd_dt=CURRENT_DATETIME()
from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` b
WHERE a.emp_active_directory_user_id=b.emp_active_directory_user_id