INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan` ( mbrs_payment_plan_adw_key,
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
    integrate_update_batch_number )
SELECT
  mbrs_payment_plan_adw_key,
  source.mbrs_adw_key,
  source.mbr_rider_adw_key,
  source.payment_plan_source_key,
  source.payment_plan_nm,
  source.payment_plan_payment_cnt,
  source.payment_plan_month,
  source.payment_nbr,
  source.payment_plan_status,
  source.payment_plan_charge_dt,
  source.safety_fund_donation_ind,
  source.payment_amt,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_transformed` source
ON
  ( source.payment_plan_source_key=hist_type_2.payment_plan_source_key
    AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbrs_payment_plan_adw_key,
    payment_plan_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_transformed`
  GROUP BY
    payment_plan_source_key) pk
ON
  (source.payment_plan_source_key=pk.payment_plan_source_key);

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