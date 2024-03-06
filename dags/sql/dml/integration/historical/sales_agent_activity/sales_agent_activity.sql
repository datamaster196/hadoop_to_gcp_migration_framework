INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity` ( saa_adw_key,
    mbr_adw_key,
    mbrs_adw_key,
    mbrs_solicit_adw_key,
    aca_sales_office,
    aca_membership_office_adw_key,
    emp_role_adw_key,
    product_adw_key,
    mbrs_payment_apd_dtl_adw_key,
    saa_source_nm,
    saa_source_key,
    saa_tran_dt,
    mbr_comm_cd,
    mbr_comm_desc,
    mbr_rider_cd,
    mbr_rider_desc,
    mbr_expiration_dt,
    rider_solicit_cd,
    rider_source_of_sale_cd,
    sales_agent_tran_cd,
    saa_dues_amt,
    saa_daily_billed_ind,
    rider_billing_category_cd,
    rider_billing_category_desc,
    mbr_typ_cd,
    saa_commsable_activity_ind,
    saa_add_on_ind,
    saa_ar_ind,
    saa_fee_typ_cd,
    safety_fund_donation_ind ,
    saa_payment_plan_ind,
    zip_cd,
    saa_daily_cnt_ind,
    daily_cnt,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  pk.saa_adw_key,
  source.mbr_adw_key,
  source.mbrs_adw_key,
  source.mbrs_solicit_adw_key,
  source.aca_sales_office,
  source.aca_membership_office_adw_key,
  source.emp_role_adw_key,
  source.product_adw_key,
  source.mbrs_payment_apd_dtl_adw_key,
  source.saa_source_nm,
  source.saa_source_key,
  source.saa_tran_dt,
  source.mbr_comm_cd,
  source.mbr_comm_desc,
  source.mbr_rider_cd,
  source.mbr_rider_desc,
  source.mbr_expiration_dt,
  source.rider_solicit_cd,
  source.rider_source_of_sale_cd,
  source.sales_agent_tran_cd,
  source.saa_dues_amt,
  source.saa_daily_billed_ind,
  source.rider_billing_category_cd,
  source.rider_billing_category_desc,
  source.mbr_typ_cd,
  source.saa_commsable_activity_ind,
  source.saa_add_on_ind,
  source.saa_ar_ind,
  source.saa_fee_typ_cd,
  source.safety_fund_donation_ind ,
  source.saa_payment_plan_ind,
  source.zip_cd,
  source.saa_daily_cnt_ind,
  source.daily_cnt,
  hist_type_2.effective_start_datetime,
  hist_type_2.effective_end_datetime,
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} as integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_work_transformed` source
ON
  ( source.saa_source_key=hist_type_2.saa_source_key
    AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS saa_adw_key,
    saa_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_work_transformed`
  GROUP BY
    saa_source_key) pk
ON
  (source.saa_source_key=pk.saa_source_key);

    --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------

  -- Orphaned foreign key check for mbr_adw_key
SELECT
  COUNT(target. mbr_adw_key ) AS mbr_adw_key_count
FROM (
  SELECT
    DISTINCT mbr_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbr_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`) source_FK_1
  WHERE
    target.mbr_adw_key = source_FK_1.mbr_adw_key)
HAVING
IF
  ((mbr_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.sales_agent_activity. FK Column: mbr_adw_key'));

  -- Orphaned foreign key check for mbrs_adw_key
SELECT
  COUNT(target. mbrs_adw_key ) AS mbrs_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`) source_FK_2
  WHERE
    target.mbrs_adw_key = source_FK_2.mbrs_adw_key)
HAVING
IF
  ((mbrs_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.sales_agent_activity. FK Column: mbrs_adw_key'));

  -- Orphaned foreign key check for mbrs_solicit_adw_key
SELECT
  COUNT(target. mbrs_solicit_adw_key ) AS mbrs_solicit_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_solicit_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_solicit_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation`) source_FK_3
  WHERE
    target.mbrs_solicit_adw_key = source_FK_3.mbrs_solicit_adw_key)
HAVING
IF
  ((mbrs_solicit_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.sales_agent_activity. FK Column: mbrs_solicit_adw_key'));
 
  -- Orphaned foreign key check for aca_membership_office_adw_key
SELECT
  COUNT(target. aca_membership_office_adw_key ) AS aca_membership_office_adw_key_count
FROM (
  SELECT
    DISTINCT aca_membership_office_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT aca_office_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) source_FK_4
  WHERE
    target.aca_membership_office_adw_key = source_FK_4.aca_office_adw_key)
HAVING
IF
  ((aca_membership_office_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.sales_agent_activity. FK Column: aca_membership_office_adw_key'));
 
  -- Orphaned foreign key check for emp_role_adw_key
SELECT
  COUNT(target.emp_role_adw_key ) AS emp_role_adw_key_count
FROM (
  SELECT
    DISTINCT emp_role_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT emp_role_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role`) source_FK_5
  WHERE
    target.emp_role_adw_key = source_FK_5.emp_role_adw_key)
HAVING
IF
  ((emp_role_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.sales_agent_activity. FK Column: emp_role_adw_key'));
  
  -- Orphaned foreign key check for product_adw_key
SELECT
  COUNT(target.product_adw_key ) AS product_adw_key_count
FROM (
  SELECT
    DISTINCT product_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT product_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`) source_FK_6
  WHERE
    target.product_adw_key = source_FK_6.product_adw_key)
HAVING
IF
  ((product_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.sales_agent_activity. FK Column: product_adw_key'));
 
  -- Orphaned foreign key check for mbrs_payment_apd_dtl_adw_key
SELECT
  COUNT( target.mbrs_payment_apd_dtl_adw_key ) AS mbrs_payment_applied_detail_count
FROM (
  SELECT
    DISTINCT mbrs_payment_apd_dtl_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_payment_apd_dtl_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`) source_FK_7
  WHERE
    target.mbrs_payment_apd_dtl_adw_key = source_FK_7.mbrs_payment_apd_dtl_adw_key)
HAVING
IF
  ((mbrs_payment_applied_detail_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.sales_agent_activity. FK Column: mbrs_payment_apd_dtl_adw_key'));
  
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    saa_source_key,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.sales_agent_activity' ) );
    
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.saa_adw_key )
FROM (
  SELECT
    saa_adw_key,
    saa_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) a
JOIN (
  SELECT
    saa_adw_key,
    saa_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`) b
ON
  a.saa_adw_key=b.saa_adw_key
  AND a.saa_source_key = b.saa_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.saa_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.sales_agent_activity' ));