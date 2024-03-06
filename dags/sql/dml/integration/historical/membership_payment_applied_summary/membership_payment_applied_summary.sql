INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary` ( mbrs_payment_apd_summ_adw_key,
    mbrs_payment_apd_rev_adw_key,
    payment_applied_source_key,
    mbrs_adw_key,
    mbrs_payment_received_adw_key,
    aca_office_adw_key,
    emp_adw_key,
    payment_applied_dt,
    payment_applied_amt,
    payment_batch_nm,
    payment_method_cd,
    payment_method_desc,
    payment_typ_cd,
    payment_typ_desc,
    payment_adj_cd,
    payment_adj_desc,
    payment_created_dtm,
    payment_paid_by_cd,
    payment_unapplied_amt,
    advance_payment_amt,
    payment_source_cd,
    payment_source_desc,
    payment_tran_typ_cd,
    payment_tran_typ_cdesc,
    adw_row_hash,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  mbrs_payment_apd_summ_adw_key,
  mbrs_payment_apd_rev_adw_key,
  source.payment_applied_source_key,
  mbrs_adw_key,
  mbrs_payment_received_adw_key,
  aca_office_adw_key,
  emp_adw_key,
  payment_applied_dt,
  payment_applied_amt,
  payment_batch_nm,
  payment_method_cd,
  payment_method_desc,
  payment_typ_cd,
  payment_typ_desc,
  payment_adj_cd,
  payment_adj_desc,
  payment_created_dtm,
  payment_paid_by_cd,
  payment_unapplied_amt,
  advance_payment_amt,
  payment_source_cd,
  payment_source_desc,
  payment_tran_typ_cd,
  payment_tran_typ_cdesc,
  source.adw_row_hash,
  hist_type_2.effective_start_datetime,
  hist_type_2.effective_end_datetime,
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_work_type_2_hist` hist_type_2
JOIN
`{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_transformed` source
ON
  ( source.payment_applied_source_key=hist_type_2.payment_applied_source_key
    AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbrs_payment_apd_summ_adw_key,
    payment_applied_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_transformed`
  GROUP BY
    payment_applied_source_key) pk
ON
  (source.payment_applied_source_key=pk.payment_applied_source_key);

  --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
 
  --2. -- Orphaned foreign key check for mbrs_adw_key
SELECT
  COUNT(target.mbrs_adw_key ) AS mbrs_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) target
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
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_summary. FK Column: mbrs_adw_key'));
  --3. -- Orphaned foreign key check for mbrs_payment_received_adw_key
SELECT
  COUNT(target.mbrs_payment_received_adw_key ) AS mbrs_payment_received_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_payment_received_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_payment_received_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`) source_FK_3
  WHERE
    target.mbrs_payment_received_adw_key = source_FK_3.mbrs_payment_received_adw_key)
HAVING
IF
  ((mbrs_payment_received_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_summary. FK Column: mbrs_payment_received_adw_key'));
  --4. -- Orphaned foreign key check for aca_office_adw_key
SELECT
  COUNT(target.aca_office_adw_key ) AS aca_office_adw_key_count
FROM (
  SELECT
    DISTINCT aca_office_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) target
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
    target.aca_office_adw_key = source_FK_4.aca_office_adw_key)
HAVING
IF
  ((aca_office_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_summary. FK Column: aca_office_adw_key'));
  --5. -- Orphaned foreign key check for emp_adw_key
SELECT
  COUNT(target.emp_adw_key ) AS emp_adw_key_count
FROM (
  SELECT
    DISTINCT emp_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) target
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
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_summary. FK Column: emp_adw_key'));
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    payment_applied_source_key,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.mbrs_payment_applied_summary' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbrs_payment_apd_summ_adw_key )
FROM (
  SELECT
    mbrs_payment_apd_summ_adw_key,
    payment_applied_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) a
JOIN (
  SELECT
    mbrs_payment_apd_summ_adw_key,
    payment_applied_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) b
ON
  a.mbrs_payment_apd_summ_adw_key=b.mbrs_payment_apd_summ_adw_key
  AND a.payment_applied_source_key = b.payment_applied_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbrs_payment_apd_summ_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.mbrs_payment_applied_summary' ));