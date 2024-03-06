MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_comment` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_comment_stage` b
  ON (a.comment_source_system_key = b.comment_source_system_key
  AND a.effective_start_datetime = b.effective_start_datetime)
WHEN NOT MATCHED THEN INSERT (
  comment_adw_key,
  created_emp_adw_key,
  resolved_emp_adw_key,
  comment_relation_adw_key,
  comment_source_system_nm,
  comment_source_system_key,
  comment_creation_dtm,
  comment_text,
  comment_resolved_dtm,
  comment_resltn_text,
  effective_start_datetime,
  effective_end_datetime,
  actv_ind,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number,
  adw_row_hash
  )
VALUES (
  b.comment_adw_key,
  b.created_emp_adw_key,
  b.resolved_emp_adw_key,
  b.comment_relation_adw_key,
  b.comment_source_system_nm,
  b.comment_source_system_key,
  b.comment_creation_dtm,
  b.comment_text,
  b.comment_resolved_dtm,
  b.comment_resltn_text,
  b.effective_start_datetime,
  b.effective_end_datetime,
  b.actv_ind,
  b.integrate_insert_datetime,
  b.integrate_insert_batch_number,
  b.integrate_update_datetime,
  b.integrate_update_batch_number,
  b.adw_row_hash
  )
WHEN MATCHED THEN UPDATE SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.actv_ind = b.actv_ind,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number;

--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for dim_comment
SELECT
        count(target.created_emp_adw_key ) AS created_emp_adw_key
FROM
       (select distinct created_emp_adw_key
        from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment`)  target
        where not exists (select 1
                          from (select distinct emp_adw_key
                                from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`) source_FK_1
                               --from `adw-dev.adw_pii.dim_employee`) source_FK_1
                                where target.created_emp_adw_key = source_FK_1.emp_adw_key)
HAVING
IF((created_emp_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_comment. FK Column: created_emp_adw_key'));

SELECT
        count(target.resolved_emp_adw_key ) AS resolved_emp_adw_key
FROM
         (select distinct resolved_emp_adw_key
         from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment`)  target
           where not exists (select 1
                          from (select distinct emp_adw_key
                                from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_employee`) source_FK_1
                                where target.resolved_emp_adw_key = source_FK_1.emp_adw_key)
HAVING
IF((resolved_emp_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_comment. FK Column: resolved_emp_adw_key'));


SELECT
          count(target.comment_relation_adw_key ) AS comment_relation_adw_key
FROM
          (select distinct comment_relation_adw_key
           from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment`)  target
            where not exists (select 1
                           from (select distinct mbrs_adw_key
                                 from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`) source_FK_1
                               where target.comment_relation_adw_key = source_FK_1.mbrs_adw_key)
HAVING
IF((comment_relation_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_comment. FK Column: comment_relation_adw_key'));

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
      (select comment_source_system_key , effective_start_datetime, count(*) as dupe_count
      from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment`
      group by 1, 2
      having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_comment'  ) );

---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.comment_adw_key ) from
  (select comment_adw_key , comment_source_system_key , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment`) a
join
    (select comment_adw_key, comment_source_system_key, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_comment`) b
on a.comment_adw_key=b.comment_adw_key
       and a.comment_source_system_key  = b.comment_source_system_key
       and a.effective_start_datetime  <= b.effective_end_datetime
       and b.effective_start_datetime  <= a.effective_end_datetime
       and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.comment_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_comment' ));