
 MERGE INTO
   `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied` a
 USING
   `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_mbrs_gl_payments_applied_work_stage` b
 ON
   (a.gl_payment_journal_source_key = b.gl_payment_journal_source_key
   AND a.effective_start_datetime = b.effective_start_datetime)
   WHEN NOT MATCHED THEN INSERT (
   mbrs_gl_pymt_applied_adw_key ,
   mbrs_adw_key ,
   aca_office_adw_key ,
   mbrs_payment_apd_summ_adw_key,  
   gl_payment_journal_source_key ,
   gl_payment_gl_desc ,
   gl_payment_account_nbr ,
   gl_payment_post_dt ,
   gl_payment_process_dt ,
   gl_payment_journal_amt ,
   gl_payment_journal_created_dtm ,
   gl_payment_journ_d_c_cd ,
   effective_start_datetime,
   effective_end_datetime,
   actv_ind,
   adw_row_hash,
   integrate_insert_datetime,
   integrate_insert_batch_number,
   integrate_update_datetime,
   integrate_update_batch_number )
 VALUES (
   b.mbrs_gl_pymt_applied_adw_key ,
   b.mbrs_adw_key ,
   b.aca_office_adw_key ,
   b.mbrs_payment_apd_summ_adw_key,  
   b.gl_payment_journal_source_key ,
   b.gl_payment_gl_desc ,
   b.gl_payment_account_nbr ,
   b.gl_payment_post_dt ,
   b.gl_payment_process_dt ,
   b.gl_payment_journal_amt ,
   b.gl_payment_journal_created_dtm ,
   b.gl_payment_journ_d_c_cd ,
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
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied`)  target
       where not exists (select 1
                      from (select distinct mbrs_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`) source_FK_1
                          where target.mbrs_adw_key = source_FK_1.mbrs_adw_key)
HAVING
 IF((mbrs_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.mbrs_gl_payments_applied. FK Column: mbrs_adw_key'));
 
  -- Orphaned foreign key check for aca_office_adw_key
 SELECT
     count(target.aca_office_adw_key ) AS aca_office_adw_key_count
 FROM
     (select distinct aca_office_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied`)  target
       where not exists (select 1
                      from (select distinct aca_office_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) source_FK_2
                          where target.aca_office_adw_key = source_FK_2.aca_office_adw_key)
HAVING
 IF((aca_office_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.mbrs_gl_payments_applied. FK Column: aca_office_adw_key '));
 
   -- Orphaned foreign key check for mbrs_payment_apd_summ_adw_key
 SELECT
     count(target. mbrs_payment_apd_summ_adw_key ) AS mbrs_payment_apd_summ_adw_key_count
 FROM
     (select distinct mbrs_payment_apd_summ_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied`)  target
       where not exists (select 1
                      from (select distinct mbrs_payment_apd_summ_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) source_FK_2
                          where target.mbrs_payment_apd_summ_adw_key = source_FK_2.mbrs_payment_apd_summ_adw_key)
HAVING
 IF((mbrs_payment_apd_summ_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.mbrs_gl_payments_applied. FK Column: mbrs_payment_apd_summ_adw_key '));
 
 ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    gl_payment_journal_source_key ,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.mbrs_gl_payments_applied' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbrs_gl_pymt_applied_adw_key )
FROM (
  SELECT
    mbrs_gl_pymt_applied_adw_key ,
    gl_payment_journal_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied`) a
JOIN (
  SELECT
    mbrs_gl_pymt_applied_adw_key ,
    gl_payment_journal_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied`) b
ON
  a.mbrs_gl_pymt_applied_adw_key=b. mbrs_gl_pymt_applied_adw_key
  AND a.gl_payment_journal_source_key = b.gl_payment_journal_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbrs_gl_pymt_applied_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.mbrs_gl_payments_applied' ));
