INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary` ( mbrs_billing_summary_adw_key,
    mbrs_adw_key,
    bill_summary_source_key,
    bill_process_dt,
    bill_notice_nbr,
    bill_amt,
    bill_credit_amt,
    bill_typ,
    mbr_expiration_dt,
    bill_paid_amt,
    bill_renewal_method,
    bill_panel_cd,
    bill_ebilling_ind,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  mbrs_billing_summary_adw_key,
  COALESCE(source.mbrs_adw_key,
    '-1'),
  SAFE_CAST(source.bill_summary_source_key AS INT64),
  CAST(substr (source.bill_process_dt,
      0,
      10) AS Date),
  CAST(source.bill_notice_nbr AS INT64),
  CAST(source.bill_amt AS NUMERIC),
  CAST(source.bill_credit_amt AS NUMERIC),
  source.bill_typ,
  CAST(substr (source.mbr_expiration_dt,
      0,
      10) AS Date),
  CAST(source.bill_paid_amt AS NUMERIC),
  source.bill_renewal_method,
  source.bill_panel_cd,
  source.bill_ebilling_ind,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  (
    CASE
      WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE
    'N'
  END
    ),
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_transformed` source
ON
  ( source.bill_summary_source_key=hist_type_2.bill_summary_source_key
    AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbrs_billing_summary_adw_key,
    bill_summary_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_transformed`
  GROUP BY
    bill_summary_source_key) pk
ON
  (source.bill_summary_source_key=pk.bill_summary_source_key);

  --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for mbrs_adw_key
SELECT
  COUNT(target.mbrs_adw_key ) AS mbrs_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`) source_FK_1
  WHERE
    target.mbrs_adw_key = source_FK_1.mbrs_adw_key)
HAVING
IF
  ((mbrs_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.membership_billing_summary. FK Column: mbrs_adw_key'));
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
  -- Please alter the fields according to Adnan's duplicate key checks. Email Adnan if you don't have the queries.
SELECT
  COUNT(1)
FROM (
  SELECT
    bill_summary_source_key,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.membership_billing_summary' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbrs_billing_summary_adw_key )
FROM (
  SELECT
    mbrs_billing_summary_adw_key,
    bill_summary_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`) a
JOIN (
  SELECT
    mbrs_billing_summary_adw_key,
    bill_summary_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`) b
ON
  a.mbrs_billing_summary_adw_key=b.mbrs_billing_summary_adw_key
  AND a.bill_summary_source_key = b.bill_summary_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbrs_billing_summary_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.membership_billing_summary' ));