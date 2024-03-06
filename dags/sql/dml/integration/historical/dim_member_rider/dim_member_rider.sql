INSERT INTO
`{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider`
(mbr_rider_adw_key,
mbrs_adw_key,
mbr_adw_key,
mbr_ar_card_adw_key,
emp_role_adw_key,
product_adw_key,
mbr_rider_source_key,
mbr_rider_status_cd,
mbr_rider_status_dtm,
mbr_rider_cancel_dt,
mbr_rider_cost_effective_dt,
mbr_rider_solicit_cd,
mbr_rider_do_not_renew_ind,
mbr_rider_dues_cost_amt,
mbr_rider_dues_adj_amt,
mbr_rider_payment_amt,
mbr_rider_paid_by_cd,
mbr_rider_future_cancel_dt,
mbr_rider_billing_category_cd,
mbr_rider_cancel_reason_cd,
mbr_rider_cancel_reason_desc,
mbr_rider_reinstate_ind,
mbr_rider_effective_dt,
mbr_rider_original_cost_amt,
mbr_rider_reinstate_reason_cd,
mbr_rider_extend_exp_amt,
mbr_rider_actual_cancel_dtm,
mbr_rider_def_key,
mbr_rider_actvtn_dt,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number)
SELECT
mbr_rider_adw_key,
SAFE_CAST(mbrs_adw_key as STRING),
SAFE_CAST(mbr_adw_key as STRING),
mbr_ar_card_adw_key,
emp_role_adw_key,
product_adw_key,
SAFE_CAST(source.mbr_rider_source_key AS INT64),
mbr_rider_status_cd,
SAFE_CAST(mbr_rider_status_dtm as DATETIME),
SAFE_CAST(TIMESTAMP(mbr_rider_cancel_dt) as DATE),
SAFE_CAST(TIMESTAMP(mbr_rider_cost_effective_dt) as DATE),
mbr_rider_solicit_cd,
mbr_rider_do_not_renew_ind,
SAFE_CAST(mbr_rider_dues_cost_amt as NUMERIC),
SAFE_CAST(mbr_rider_dues_adj_amt as NUMERIC),
SAFE_CAST(mbr_rider_payment_amt as NUMERIC),
mbr_rider_paid_by_cd,
SAFE_CAST(TIMESTAMP(mbr_rider_future_cancel_dt) as DATE),
mbr_rider_billing_category_cd,
mbr_rider_cancel_reason_cd,
mbr_rider_cancel_reason_desc,
mbr_rider_reinstate_ind,
SAFE_CAST(TIMESTAMP(mbr_rider_effective_dt) as DATE),
SAFE_CAST(mbr_rider_original_cost_amt as NUMERIC),
mbr_rider_reinstate_reason_cd,
SAFE_CAST(mbr_rider_extend_exp_amt as NUMERIC),
SAFE_CAST(mbr_rider_actual_cancel_dtm as DATETIME),
SAFE_CAST(mbr_rider_def_key as INT64),
SAFE_CAST(TIMESTAMP(mbr_rider_actvtn_dt) as DATE),
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
CASE WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'ELSE 'N' END AS actv_ind,
source.adw_row_hash,
CURRENT_DATETIME(),
{{ dag_run.id }},
CURRENT_DATETIME(),
{{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_transformed` source ON (
  source.mbr_rider_source_key=hist_type_2.mbr_rider_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbr_rider_adw_key,
    mbr_rider_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_transformed`
  GROUP BY
    mbr_rider_source_key) pk
ON (source.mbr_rider_source_key=pk.mbr_rider_source_key);

   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for mbrs_adw_key

SELECT
     count(target.mbrs_adw_key) AS mbrs_adw_key_count
 FROM
     (select distinct mbrs_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`)  target
       where not exists (select 1
                      from (select distinct mbrs_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`) source_FK_1
                          where target.mbrs_adw_key = source_FK_1.mbrs_adw_key)
HAVING
 IF((mbrs_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member_rider. FK Column: mbrs_adw_key'));
 
  -- Orphaned foreign key check for mbr_adw_key
 SELECT
     count(target.mbr_adw_key ) AS mbr_adw_key_count
 FROM
     (select distinct mbr_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`)  target
       where not exists (select 1
                      from (select distinct mbr_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`) source_FK_2
                          where target.mbr_adw_key = source_FK_2.mbr_adw_key)
HAVING
 IF((mbr_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member_rider. FK Column: mbr_adw_key '));

  -- Orphaned foreign key check for mbr_ar_card_adw_key
 SELECT
     count(target.mbr_ar_card_adw_key ) AS mbr_ar_card_adw_key_count
 FROM
     (select distinct mbr_ar_card_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`)  target
       where not exists (select 1
                      from (select distinct mbr_ar_card_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_auto_renewal_card`) source_FK_3
                          where target.mbr_ar_card_adw_key = source_FK_3.mbr_ar_card_adw_key)
HAVING
 IF((mbr_ar_card_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member_rider. FK Column: mbr_ar_card_adw_key'));

  -- Orphaned foreign key check for emp_role_adw_key
 SELECT
     count(target.emp_role_adw_key ) AS emp_role_adw_key_count
 FROM
     (select distinct emp_role_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`)  target
       where not exists (select 1
                      from (select distinct emp_role_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`) source_FK_4
                          where target.emp_role_adw_key = source_FK_4.emp_role_adw_key)
HAVING
 IF((emp_role_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member_rider. FK Column: emp_role_adw_key'));
 
  -- Orphaned foreign key check for product_adw_key
 SELECT
     count(target.product_adw_key ) AS product_adw_key_count
 FROM
     (select distinct product_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`)  target
       where not exists (select 1
                      from (select distinct product_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) source_FK_5
                          where target.product_adw_key = source_FK_5.product_adw_key)
HAVING
 IF((product_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member_rider. FK Column: product_adw_key'));

--   -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    mbr_rider_source_key ,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.dim_member_rider' ));
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbr_rider_adw_key)
FROM (
  SELECT
    mbr_rider_adw_key,
    mbr_rider_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`) a
JOIN (
  SELECT
    mbr_rider_adw_key ,
    mbr_rider_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member_rider`) b
ON
  a.mbr_rider_adw_key=b.mbr_rider_adw_key
  AND a.mbr_rider_source_key = b.mbr_rider_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbr_rider_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_member_rider' ));