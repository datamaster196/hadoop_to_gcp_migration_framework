INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`
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
)
SELECT
mbrs_payment_received_adw_key,
source.mbrs_adw_key,
source.mbrs_billing_summary_adw_key,
source.mbr_ar_card_adw_key,
source.aca_office_adw_key,
source.emp_adw_key,
SAFE_CAST(source.BATCH_PAYMENT_KY AS INT64),
SAFE_CAST(source.BATCH_KY AS INT64),
source.BATCH_NAME,
SAFE_CAST(source.EXP_CT AS INT64),
SAFE_CAST(source.EXP_AT AS NUMERIC),
SAFE_CAST(source.CREATE_DT AS DATETIME),
source.STATUS,
SAFE_CAST(source.STATUS_DT AS DATETIME),
SAFE_CAST(source.PAYMENT_AT AS NUMERIC),
source.PAYMENT_METHOD_CD,
source.payment_received_method_desc,
source.PAYMENT_SOURCE_CD,
source.payment_source_desc,
source.TRANSACTION_TYPE_CD,
source.payment_typ_desc,
source.REASON_CD,
source.PAID_BY_CD,
source.ADJUSTMENT_DESCRIPTION_CD,
source.payment_adj_desc,
source.POST_COMPLETED_FL,
SAFE_CAST(source.POST_DT AS DATETIME),
source.AUTO_RENEW_FL,
CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
    ( CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END) ,
    source.adw_row_hash,
    CURRENT_DATETIME() ,
    {{ dag_run.id }} ,
    CURRENT_DATETIME() ,
    {{ dag_run.id }}
FROM
`{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_transformed` source
  ON (
  source.BATCH_PAYMENT_KY=hist_type_2.BATCH_PAYMENT_KY
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbrs_payment_received_adw_key,
    BATCH_PAYMENT_KY
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_transformed`
  GROUP BY
    BATCH_PAYMENT_KY) pk
ON (source.BATCH_PAYMENT_KY=pk.BATCH_PAYMENT_KY);

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
