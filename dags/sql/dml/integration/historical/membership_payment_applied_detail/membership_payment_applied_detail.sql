INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail` 
  ( mbrs_payment_apd_dtl_adw_key,	
	mbr_adw_key,	
	mbrs_payment_apd_summ_adw_key,	
	product_adw_key,	
	payment_apd_dtl_source_key,	
	payment_applied_dt,	
	payment_method_cd,	
	payment_method_desc,	
	payment_detail_amt,	
	payment_applied_unapplied_amt,	
	discount_amt,	
	pymt_apd_discount_counted,	
	discount_effective_dt,	
	rider_cost_effective_dtm,	
	effective_start_datetime,	
	effective_end_datetime,	
	actv_ind,	
	adw_row_hash,	
	integrate_insert_datetime,	
	integrate_insert_batch_number,	
	integrate_update_datetime,	
	integrate_update_batch_number )
SELECT
  mbrs_payment_apd_dtl_adw_key,
  source.mbr_adw_key,
  CAST(source.mbrs_payment_apd_summ_adw_key AS string),
  CAST (source.product_adw_key AS string),	  
  CAST(source.payment_apd_dtl_source_key AS int64),
  source.payment_applied_dt,
  CAST(source.payment_method_cd AS string),
  source.payment_method_desc,
  CAST(source.payment_detail_amt AS numeric),
  CAST(source.payment_applied_unapplied_amt AS numeric),
  CAST(source.discount_amt AS numeric),
  CAST(source.pymt_apd_discount_counted AS string),
  CAST(substr(source.discount_effective_dt, 0, 10) AS date),
  CAST(source.rider_cost_effective_dtm AS datetime),
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_transformed` source ON (
  source.payment_apd_dtl_source_key=hist_type_2.payment_apd_dtl_source_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbrs_payment_apd_dtl_adw_key,
    payment_apd_dtl_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_transformed`
  GROUP BY
    payment_apd_dtl_source_key) pk
ON (source.payment_apd_dtl_source_key=pk.payment_apd_dtl_source_key);

  --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for mbr_adw_key
SELECT
  COUNT(target.mbr_adw_key ) AS mbr_adw_key_count
FROM (
  SELECT
    DISTINCT mbr_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`) target
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
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_detail. FK Column: mbr_adw_key'));
    
      
   -- Orphaned foreign key check for mbrs_payment_apd_summ_adw_key
SELECT
  COUNT(target.mbrs_payment_apd_summ_adw_key ) AS mbrs_payment_apd_summ_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_payment_apd_summ_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_payment_apd_summ_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) source_FK_2
  WHERE
    target.mbrs_payment_apd_summ_adw_key = source_FK_2.mbrs_payment_apd_summ_adw_key)
HAVING
IF
  ((mbrs_payment_apd_summ_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_detail. FK Column: mbrs_payment_apd_summ_adw_key'));     
    
 -- Orphaned foreign key check for product_adw_key
SELECT
  COUNT(target.product_adw_key ) AS product_adw_key_count
FROM (
  SELECT
    DISTINCT product_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`) target
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
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_detail. FK Column: product_adw_key'));       
    
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    payment_apd_dtl_source_key ,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.mbrs_payment_applied_detail' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbrs_payment_apd_dtl_adw_key )
FROM (
  SELECT
    mbrs_payment_apd_dtl_adw_key ,
    payment_apd_dtl_source_key ,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`) a
JOIN (
  SELECT
    mbrs_payment_apd_dtl_adw_key,
    payment_apd_dtl_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`) b
ON
  a.mbrs_payment_apd_dtl_adw_key=b. mbrs_payment_apd_dtl_adw_key
  AND a.payment_apd_dtl_source_key = b.payment_apd_dtl_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbrs_payment_apd_dtl_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.mbrs_payment_applied_detail' ));
