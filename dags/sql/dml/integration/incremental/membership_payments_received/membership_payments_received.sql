MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_stage` b
  ON
    (a.mbrs_payment_received_adw_key = b.mbrs_payment_received_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT
    (
    mbrs_payment_received_adw_key,
mbrs_adw_key,
mbrs_billing_summary_adw_key,
mbr_ar_card_adw_key,
aca_office_adw_key,
emp_adw_key,
payment_received_source_key,
payment_received_batch_key,
payment_received_nm,
payment_received_cnt,
payment_received_expected_amt,
payment_received_created_dtm,
payment_received_status,
payment_received_status_dtm,
payment_received_amt,
payment_received_method_cd,
payment_received_method_desc,
payment_source_cd,
payment_source_desc,
payment_typ_cd,
payment_typ_desc,
payment_reason_cd,
payment_paid_by_desc,
payment_adj_cd,
payment_adj_desc,
payment_received_post_ind,
payment_received_post_dtm,
payment_received_ar_ind,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
    ) VALUES
  (  b.mbrs_payment_received_adw_key,
b.mbrs_adw_key,
b.mbrs_billing_summary_adw_key,
b.mbr_ar_card_adw_key,
b.aca_office_adw_key,
b.emp_adw_key,
b.payment_received_source_key,
b.payment_received_batch_key,
b.payment_received_nm,
b.payment_received_cnt,
b.payment_received_expected_amt,
b.payment_received_created_dtm,
b.payment_received_status,
b.payment_received_status_dtm,
b.payment_received_amt,
b.payment_received_method_cd,
b.payment_received_method_desc,
b.payment_source_cd,
b.payment_source_desc,
b.payment_typ_cd,
b.payment_typ_desc,
b.payment_reason_cd,
b.payment_paid_by_desc,
b.payment_adj_cd,
b.payment_adj_desc,
b.payment_received_post_ind,
b.payment_received_post_dtm,
b.payment_received_ar_ind,
b.effective_start_datetime,
b.effective_end_datetime,
b.actv_ind,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number)
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
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`)  target
       where not exists (select 1
                      from (select distinct mbrs_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`) source_FK_1
                          where target.mbrs_adw_key = source_FK_1.mbrs_adw_key)
HAVING
 IF((mbrs_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.membership_payment_received. FK Column: mbrs_adw_key'));
 
   -- Orphaned foreign key check for mbrs_billing_summary_adw_key
 SELECT
     count(target. mbrs_billing_summary_adw_key ) AS mbrs_billing_summary_adw_key_count
 FROM
     (select distinct mbrs_billing_summary_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`)  target
       where not exists (select 1
                      from (select distinct mbrs_billing_summary_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`) source_FK_2
                          where target.mbrs_billing_summary_adw_key = source_FK_2.mbrs_billing_summary_adw_key)
HAVING
 IF((mbrs_billing_summary_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.membership_payment_received. FK Column: mbrs_billing_summary_adw_key '));
    -- Orphaned foreign key check for mbr_ar_card_adw_key
 SELECT
     count(target. mbr_ar_card_adw_key ) AS mbr_ar_card_adw_key_count
 FROM
     (select distinct mbr_ar_card_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`)  target
       where not exists (select 1
                      from (select distinct mbr_ar_card_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card`) source_FK_3
                          where target.mbr_ar_card_adw_key = source_FK_3.mbr_ar_card_adw_key)
HAVING
 IF((mbr_ar_card_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.membership_payment_received. FK Column: mbr_ar_card_adw_key '));
 
  -- Orphaned foreign key check for aca_office_adw_key
 SELECT
     count(target.aca_office_adw_key ) AS aca_office_adw_key_count
 FROM
     (select distinct aca_office_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`)  target
       where not exists (select 1
                      from (select distinct aca_office_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) source_FK_4
                          where target.aca_office_adw_key = source_FK_4.aca_office_adw_key)
HAVING
 IF((aca_office_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.membership_payment_received. FK Column: aca_office_adw_key '));
   --5. -- Orphaned foreign key check for emp_adw_key
SELECT
  COUNT(target.emp_adw_key ) AS emp_adw_key_count
FROM (
  SELECT
    DISTINCT emp_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT emp_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee`) source_FK_5
  WHERE
    target.emp_adw_key = source_FK_5.emp_adw_key)
HAVING
IF
  ((emp_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.membership_payment_received. FK Column: emp_adw_key'));
 ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    payment_received_source_key ,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.membership_payment_received' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbrs_payment_received_adw_key )
FROM (
  SELECT
    mbrs_payment_received_adw_key ,
    payment_received_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`) a
JOIN (
  SELECT
    mbrs_payment_received_adw_key ,
    payment_received_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`) b
ON
  a. mbrs_payment_received_adw_key =b. mbrs_payment_received_adw_key
  AND a. payment_received_source_key = b.payment_received_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbrs_payment_received_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.membership_payment_received' ));