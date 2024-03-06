MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_detail_work_final_stage` b
    ON (a.mbrs_billing_detail_adw_key = b.mbrs_billing_detail_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
mbrs_billing_detail_adw_key,
mbrs_billing_summary_adw_key,
mbr_adw_key,
product_adw_key,
bill_detail_source_key,
bill_summary_source_key,
bill_process_dt,
bill_notice_nbr,
bill_typ,
bill_detail_amt,
bill_detail_text,
rider_billing_category_cd,
rider_billing_category_desc,
rider_solicit_cd,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
    ) VALUES
    (
b.mbrs_billing_detail_adw_key,
COALESCE(b.mbrs_billing_summary_adw_key,'-1'),
COALESCE(b.mbr_adw_key,'-1'),
COALESCE(b.product_adw_key,'-1'),
b.bill_detail_source_key,
b.bill_summary_source_key,
b.bill_process_dt,
b.bill_notice_nbr,
b.bill_typ,
b.bill_detail_amt,
b.bill_detail_text,
b.rider_billing_category_cd,
b.rider_billing_category_desc,
b.rider_solicit_cd,
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
  -- Orphaned foreign key check for mbrs_billing_summary_adw_key
SELECT
  COUNT(target. mbrs_billing_summary_adw_key ) AS mbrs_billing_summary_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_billing_summary_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_billing_summary_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`) source_FK_1
  WHERE
    target.mbrs_billing_summary_adw_key = source_FK_1.mbrs_billing_summary_adw_key)
HAVING
IF
  ((mbrs_billing_summary_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.membership_billing_detail. FK Column: mbrs_billing_summary_adw_key'));
    
      
   -- Orphaned foreign key check for mbr_adw_key
SELECT
  COUNT(target.mbr_adw_key ) AS mbr_adw_key_count
FROM (
  SELECT
    DISTINCT mbr_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbr_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`) source_FK_2
  WHERE
    target.mbr_adw_key = source_FK_2.mbr_adw_key)
HAVING
IF
  ((mbr_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.membership_billing_detail. FK Column: mbr_adw_key'));
 -- Orphaned foreign key check for product_adw_key
SELECT
  COUNT(target.product_adw_key ) AS product_adw_key_count
FROM (
  SELECT
    DISTINCT product_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT product_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`) source_FK_3
  WHERE
    target.product_adw_key = source_FK_3.product_adw_key)
HAVING
IF
  ((product_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.membership_billing_detail. FK Column: product_adw_key'));       
    
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    bill_detail_source_key ,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.membership_billing_detail' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbrs_billing_detail_adw_key )
FROM (
  SELECT
    mbrs_billing_detail_adw_key ,
    bill_detail_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail`) a
JOIN (
  SELECT
    mbrs_billing_detail_adw_key ,
    bill_detail_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail`) b
ON
  a.mbrs_billing_detail_adw_key=b. mbrs_billing_detail_adw_key
  AND a.bill_detail_source_key = b.bill_detail_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbrs_billing_detail_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.membership_billing_detail' ));