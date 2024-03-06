MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_stage` b
    ON (a.mbrs_solicit_adw_key = b.mbrs_solicit_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
    mbrs_solicit_adw_key,
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
integrate_update_batch_number)
VALUES
(
b.mbrs_solicit_adw_key,
b.mbrs_solicit_source_system_key,
b.solicit_cd,
b.solicit_group_cd,
b.mbr_typ_cd,
b.solicit_camp_cd,
b.solicit_category_cd,
b.solicit_discount_cd,
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
-- No Orphaned foreign key check for dim_membership_solicitation

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
    (select mbrs_solicit_source_system_key , solicit_cd, effective_start_datetime, count(*) as dupe_count
     from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation`
     group by 1, 2, 3
     having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_membership_solicitation'  ) );


---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.mbrs_solicit_adw_key ) from
   (select mbrs_solicit_adw_key , mbrs_solicit_source_system_key, effective_start_datetime, effective_end_datetime from
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation`) a
join
   (select mbrs_solicit_adw_key, mbrs_solicit_source_system_key, effective_start_datetime, effective_end_datetime from
   `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation`) b
on a.mbrs_solicit_adw_key=b.mbrs_solicit_adw_key
        and a.mbrs_solicit_source_system_key = b.mbrs_solicit_source_system_key
        and a.effective_start_datetime  <= b.effective_end_datetime
        and b.effective_start_datetime  <= a.effective_end_datetime
        and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.mbrs_solicit_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_membership_solicitation' ));