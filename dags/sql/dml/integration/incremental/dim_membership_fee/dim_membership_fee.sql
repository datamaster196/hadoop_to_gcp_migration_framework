MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_stage` b
ON
  (a.mbrs_fee_source_key = b.mbrs_fee_source_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  mbrs_fee_adw_key, 
  mbr_adw_key , 
  product_adw_key , 
  mbrs_fee_source_key , 
  status , 
  fee_typ , 
  fee_dt , 
  waived_dt,
  waived_by,
  donor_nbr,
  waived_reason_cd,
  effective_start_datetime, 
  effective_end_datetime, 
  actv_ind, 
  adw_row_hash, 
  integrate_insert_datetime, 
  integrate_insert_batch_number, 
  integrate_update_datetime, 
  integrate_update_batch_number ) VALUES (
  b.mbrs_fee_adw_key , 
  b.mbr_adw_key , 
  b.product_adw_key , 
  b.mbrs_fee_source_key , 
  b.status , 
  b.fee_typ , 
  b.fee_dt , 
  b.waived_dt,
  b.waived_by,
  b.donor_nbr,
  b.waived_reason_cd,
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
  -- Orphaned foreign key check for mbr_adw_key

SELECT
     count(target.mbr_adw_key) AS mbr_adw_key_count
 FROM
     (select distinct mbr_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`)  target
       where not exists (select 1
                      from (select distinct mbr_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`) source_FK_1
                          where target.mbr_adw_key = source_FK_1.mbr_adw_key)
HAVING
 IF((mbr_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership_fee. FK Column: mbr_adw_key'));
 
  -- Orphaned foreign key check for  product_adw_key
 SELECT
     count(target.product_adw_key ) AS product_adw_key_count
 FROM
     (select distinct product_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`)  target
       where not exists (select 1
                      from (select distinct product_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) source_FK_2
                          where target.product_adw_key = source_FK_2.product_adw_key)
HAVING
 IF((product_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership_fee. FK Column: product_adw_key'));

----------------------------------------------------------------------------------------------
-- Duplicate Checks

 select count(1)
   from
   (select mbrs_fee_source_key , effective_start_datetime, count(*) as dupe_count
   from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`
   group by 1, 2
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_membership_fee'  ) );

 ---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a.mbrs_fee_adw_key ) from
 (select mbrs_fee_adw_key , mbrs_fee_source_key , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`) a
 join
 (select mbrs_fee_adw_key, mbrs_fee_source_key, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`) b
 on a.mbrs_fee_adw_key=b.mbrs_fee_adw_key
    and a.mbrs_fee_source_key  = b.mbrs_fee_source_key
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.mbrs_fee_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_membership_fee' ));
