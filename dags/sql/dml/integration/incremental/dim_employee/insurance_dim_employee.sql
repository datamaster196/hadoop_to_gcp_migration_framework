MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_stage` b
ON a.emp_adw_key=b.emp_adw_key
AND a.effective_start_datetime = b.effective_start_datetime
when not matched then INSERT(
    emp_adw_key,
    active_directory_email_adw_key,
    email_adw_key,
    phone_adw_key,
    fax_phone_adw_key,
    nm_adw_key,
    emp_typ_cd,
    emp_active_directory_user_id,
    emp_hr_id,
    emp_hr_hire_dt	,
    emp_hr_term_dt,
    emp_hr_term_reason,
    emp_hr_status,
    emp_hr_region,
    emp_loc_hr,
    emp_hr_title,
    emp_hr_sprvsr,
    emp_hr_job_cd,
    emp_hr_cost_center,
    emp_hr_position_typ,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number,
    adw_row_hash
)
VALUES(
    b.emp_adw_key,
    b.active_directory_email_adw_key,
    b.email_adw_key,
    b.phone_adw_key,
    b.fax_phone_adw_key,
    b.nm_adw_key,
    b.emp_typ_cd,
    b.emp_active_directory_user_id,
    b.emp_hr_id,
    b.emp_hr_hire_dt	,
    b.emp_hr_term_dt,
    b.emp_hr_term_reason,
    b.emp_hr_status,
    b.emp_hr_region,
    b.emp_loc_hr,
    b.emp_hr_title,
    b.emp_hr_sprvsr,
    b.emp_hr_job_cd,
    b.emp_hr_cost_center,
    b.emp_hr_position_typ,
    b.effective_start_datetime,
    b.effective_end_datetime,
    b.actv_ind,
    b.integrate_insert_datetime	,
    b.integrate_insert_batch_number	,
    b.integrate_update_datetime	,
    b.integrate_update_batch_number,
    b.adw_row_hash
)
WHEN MATCHED THEN UPDATE
SET
a.effective_end_datetime = b.effective_end_datetime,
  a.actv_ind = b.actv_ind,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number;



  --------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for dim_employee
SELECT
       count(target.active_directory_email_adw_key ) AS active_directory_email_adw_key
FROM
       (select distinct active_directory_email_adw_key
       from  `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`) target
       where not exists (select 1
                        from (select distinct email_adw_key
                              from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_email`) source_FK_1
                              where target.active_directory_email_adw_key = source_FK_1.email_adw_key)
  HAVING
  IF((active_directory_email_adw_key = 0  ), true, ERROR('Error: FK check failed for adw_pii.dim_employee. FK Column: active_directory_email_adw_key'));

SELECT
      count(target.email_adw_key ) AS email_adw_key
FROM
         (select distinct email_adw_key
         from  `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`) target  
          where not exists (select 1
                         from (select distinct email_adw_key
                               from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_email`) source_FK_1
                               where target.email_adw_key = source_FK_1.email_adw_key)
HAVING
IF((email_adw_key = 0  ), true, ERROR('Error: FK check failed for adw_pii.dim_employee. FK Column: email_adw_key'));

SELECT
      count(target.phone_adw_key ) AS phone_adw_key
FROM
      (select distinct phone_adw_key
      from  `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`)  target
      where not exists (select 1
                         from (select distinct phone_adw_key
                               from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_phone`) source_FK_1
                               where target.phone_adw_key = source_FK_1.phone_adw_key)
HAVING
IF((phone_adw_key = 0  ), true, ERROR('Error: FK check failed for adw_pii.dim_employee. FK Column: phone_adw_key'));

SELECT
        count(target.fax_phone_adw_key ) AS fax_phone_adw_key
FROM
        (select distinct fax_phone_adw_key
         from  `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`)  target
          where not exists (select 1
                         from (select distinct phone_adw_key
                               from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_phone`) source_FK_1
                               where target.fax_phone_adw_key = source_FK_1.phone_adw_key)
HAVING
IF((fax_phone_adw_key = 0  ), true, ERROR('Error: FK check failed for adw_pii.dim_employee. FK Column: fax_phone_adw_key'));

SELECT
        count(target.nm_adw_key ) AS nm_adw_key
FROM
        (select distinct nm_adw_key
         from  `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`)  target
          where not exists (select 1
                         from (select distinct nm_adw_key
                               from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_name`) source_FK_1
                               where target.nm_adw_key = source_FK_1.nm_adw_key)
HAVING
IF((nm_adw_key = 0  ), true, ERROR('Error: FK check failed for adw_pii.dim_employee. FK Column: nm_adw_key'));

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
     (select emp_active_directory_user_id, effective_start_datetime, count(*) as dupe_count
     from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`
     group by 1, 2
     having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw_pii.dim_employee'  ) );

---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.emp_adw_key )
from
   (select emp_adw_key , emp_active_directory_user_id , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`) a
join
   (select emp_adw_key, emp_active_directory_user_id, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`) b
on a.emp_adw_key=b.emp_adw_key
      and a.emp_active_directory_user_id  = b.emp_active_directory_user_id
      and a.effective_start_datetime  <= b.effective_end_datetime
      and b.effective_start_datetime  <= a.effective_end_datetime
      and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.emp_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw_pii.dim_employee' ));