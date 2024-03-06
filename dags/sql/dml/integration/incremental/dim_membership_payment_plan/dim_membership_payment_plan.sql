MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_stage` b
ON
  (a.mbrs_payment_plan_adw_key = b.mbrs_payment_plan_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  mbrs_payment_plan_adw_key ,    
  mbrs_adw_key,
  mbr_rider_adw_key,
  payment_plan_source_key,
  payment_plan_nm,
  payment_plan_payment_cnt,
  payment_plan_month,
  payment_nbr,
  payment_plan_status,
  payment_plan_charge_dt,
  safety_fund_donation_ind ,
  payment_amt,
  effective_start_datetime,
  effective_end_datetime,
  actv_ind,
  adw_row_hash,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number

  ) VALUES (
  b.mbrs_payment_plan_adw_key ,    
  b.mbrs_adw_key,
  b.mbr_rider_adw_key,
  b.payment_plan_source_key,
  b.payment_plan_nm,    
  b.payment_plan_payment_cnt,
  b.payment_plan_month,
  b.payment_nbr,
  b.payment_plan_status,
  b.payment_plan_charge_dt,
  b.safety_fund_donation_ind ,
  b.payment_amt,
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
  -- Orphaned foreign key check for mbrs_adw_key

SELECT
     count(target.mbrs_adw_key) AS mbrs_adw_key_count
 FROM
     (select distinct mbrs_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan`)  target
       where not exists (select 1
                      from (select distinct mbrs_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`) source_FK_1
                          where target.mbrs_adw_key = source_FK_1.mbrs_adw_key)
HAVING
 IF((mbrs_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership_payment_plan. FK Column: mbrs_adw_key'));
 
  -- Orphaned foreign key check for  mbr_rider_adw_key
 SELECT
     count(target.mbr_rider_adw_key ) AS mbr_rider_adw_key_count
 FROM
     (select distinct mbr_rider_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan`)  target
       where not exists (select 1
                      from (select distinct mbr_rider_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider`) source_FK_2
                          where target.mbr_rider_adw_key = source_FK_2.mbr_rider_adw_key)
HAVING
 IF((mbr_rider_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership_payment_plan. FK Column: mbr_rider_adw_key'));
 
 
 ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    payment_plan_source_key,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.dim_membership_payment_plan' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbrs_payment_plan_adw_key)
FROM (
  SELECT
    mbrs_payment_plan_adw_key,
    payment_plan_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan`) a
JOIN (
  SELECT
    mbrs_payment_plan_adw_key,
    payment_plan_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan`) b
ON
  a.mbrs_payment_plan_adw_key=b.mbrs_payment_plan_adw_key
  AND a.payment_plan_source_key = b.payment_plan_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbrs_payment_plan_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_membership_payment_plan' ));