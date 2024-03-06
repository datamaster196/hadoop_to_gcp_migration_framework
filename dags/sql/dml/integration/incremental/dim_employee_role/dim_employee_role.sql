MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_role_work_stage` b
ON
  (a. emp_role_adw_key = b. emp_role_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  emp_role_adw_key , 
  emp_adw_key , 
  nm_adw_key , 
  aca_office_adw_key, 
  emp_biz_role_line_cd  , 
  emp_role_id , 
  emp_role_typ , 
  effective_start_datetime, 
  effective_end_datetime, 
  actv_ind, 
  adw_row_hash, 
  integrate_insert_datetime, 
  integrate_insert_batch_number, 
  integrate_update_datetime, 
  integrate_update_batch_number ) VALUES (
  b.emp_role_adw_key , 
  b.emp_adw_key , 
  b.nm_adw_key , 
  b.aca_office_adw_key, 
  b.emp_biz_role_line_cd  , 
  b.emp_role_id , 
  b.emp_role_typ , 
  b.effective_start_datetime, 
  b.effective_end_datetime, 
  b.actv_ind, 
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
  a.actv_ind = b.actv_ind,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number;

--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for dim_employee_role
SELECT
        count(target.emp_adw_key ) AS emp_adw_key
FROM
        (select distinct emp_adw_key
        from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`)  target
          where not exists (select 1
                         from (select distinct emp_adw_key
                               from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`) source_FK_1
                               where target.emp_adw_key = source_FK_1.emp_adw_key)
HAVING
IF((emp_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_employee_role. FK Column: emp_adw_key'));

SELECT
         count(target.nm_adw_key ) AS nm_adw_key
FROM
         (select distinct nm_adw_key
          from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`)  target
          where not exists (select 1
                          from (select distinct nm_adw_key
                                from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_name`) source_FK_1
                                where target.nm_adw_key = source_FK_1.nm_adw_key)
HAVING
IF((nm_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_employee_role. FK Column: nm_adw_key'));

SELECT
        count(target.aca_office_adw_key ) AS aca_office_adw_key
FROM
         (select distinct aca_office_adw_key
          from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`)  target
          where not exists (select 1
                          from (select distinct aca_office_adw_key
                                from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office`) source_FK_1
                              where target.aca_office_adw_key = source_FK_1.aca_office_adw_key)
HAVING
IF((aca_office_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_employee_role. FK Column: aca_office_adw_key'));

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
      (select emp_role_id , effective_start_datetime, count(*) as dupe_count
       from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`
       group by 1, 2
       having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_employee_role'  ) );

---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.emp_role_adw_key )
from
 (select emp_role_adw_key , emp_role_id , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`) a
join
 (select emp_role_adw_key, emp_role_id, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`) b
on a.emp_role_adw_key=b.emp_role_adw_key
      and a.emp_role_id  = b.emp_role_id
      and a.effective_start_datetime  <= b.effective_end_datetime
      and b.effective_start_datetime  <= a.effective_end_datetime
      and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.emp_role_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_employee_role' ));