CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_transformed` AS
SELECT
  SAFE_CAST(membership_payment_ky AS int64) AS payment_applied_source_key,
  COALESCE(aca_office.aca_office_adw_key,
    '-1') AS aca_office_adw_key,
  COALESCE(membership.mbrs_adw_key,
    '-1') AS mbrs_adw_key,
  SAFE_CAST (SAFE_CAST(payment_dt AS datetime) AS date) AS payment_applied_dt,
  SAFE_CAST(payment_at AS numeric) AS payment_applied_amt,
  batch_name AS payment_batch_nm,
  source.payment_method_cd AS payment_method_cd,
  cx_codes1.code_desc AS payment_method_desc,
  source.transaction_type_cd AS payment_typ_cd,
  cx_codes2.code_desc AS payment_typ_desc,
  adjustment_description_cd AS payment_adj_cd,
  cx_codes3.code_desc AS payment_adj_desc,
  SAFE_CAST(create_dt AS datetime) AS payment_created_dtm,
  CASE paid_by_cd
    WHEN 'P' THEN 'Primary'
    WHEN 'D' THEN 'Donor'
    WHEN 'A' THEN 'Associate'
END
  AS payment_paid_by_cd,
  COALESCE(rev_membership_payment_ky,
    '-1') AS mbrs_payment_apd_rev_adw_key,
  COALESCE(employee.emp_adw_key,
    '-1') AS emp_adw_key,
  SAFE_CAST(unapplied_at AS numeric) AS payment_unapplied_amt,
  COALESCE(SAFE_CAST(source.advance_pay_at  AS numeric),0) AS advance_payment_amt,
  COALESCE(source.source_cd,'') AS payment_source_cd,
  COALESCE(cx_codes4.code_desc,'') AS payment_source_desc,
  COALESCE(source.transaction_type_cd,'') AS payment_tran_typ_cd,
  CASE source.transaction_type_cd
    WHEN 'Z' THEN 'Reject'
    ELSE COALESCE(cx_codes5.code_desc,'')
  END AS payment_tran_typ_cdesc,
  COALESCE(payments_received.mbrs_payment_received_adw_key,
    '-1') AS mbrs_payment_received_adw_key,
  TO_BASE64(MD5(CONCAT(COALESCE(payment_dt,
          ''),COALESCE( payment_at,
          ''),COALESCE( payment_dt,
          ''),COALESCE( batch_name,
          ''),COALESCE( source.payment_method_cd,
          ''),COALESCE( source.transaction_type_cd,
          ''),COALESCE( adjustment_description_cd,
          ''),COALESCE( create_dt,
          ''),COALESCE( rev_membership_payment_ky,
          '-1'),COALESCE( cx_codes1.code_desc,
          ''),COALESCE( cx_codes2.code_desc,
          ''),COALESCE( cx_codes3.code_desc,
          ''),COALESCE( cx_codes4.code_desc,
          ''),COALESCE( cx_codes5.code_desc,
          ''),COALESCE( unapplied_at,
          ''),COALESCE( membership_payment_ky,
          '-1'),COALESCE( membership.mbrs_adw_key,
          '-1'),COALESCE( rev_membership_payment_ky,
          '-1'),COALESCE( employee.emp_adw_key,
          '-1'),COALESCE( payments_received.mbrs_payment_received_adw_key,
          '-1') ))) AS adw_row_hash,
  SAFE_CAST(source.last_upd_dt AS DATETIME) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} as integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_source` AS source
LEFT JOIN
  ( SELECT 	code,
			code_desc,
			ROW_NUMBER() OVER(PARTITION BY code ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
  FROM `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
  WHERE code_type='PEMTH' ) cx_codes1
ON
  ( source.payment_method_cd=cx_codes1.code
    AND cx_codes1.dupe_check = 1 )
LEFT JOIN
  ( SELECT 	code,
			code_desc,
			ROW_NUMBER() OVER(PARTITION BY code ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
  FROM `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
  WHERE code_type='PAYTYP' ) cx_codes2
ON
  ( source.transaction_type_cd=cx_codes2.code
    AND cx_codes2.dupe_check = 1 )
LEFT JOIN
  ( SELECT 	code,
			code_desc,
			ROW_NUMBER() OVER(PARTITION BY code ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
  FROM `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
  WHERE code_type='PAYADJ' ) cx_codes3
ON
  ( source.adjustment_description_cd=cx_codes3.code
    AND cx_codes3.dupe_check = 1 )
LEFT JOIN
  ( SELECT 	code,
			code_desc,
			ROW_NUMBER() OVER(PARTITION BY code ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
  FROM `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
  WHERE code_type='PAYSRC' ) cx_codes4
ON
  ( source.source_cd=cx_codes4.code
	AND cx_codes4.dupe_check = 1	)
LEFT JOIN
  ( SELECT 	code,
			code_desc,
			ROW_NUMBER() OVER(PARTITION BY code ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
  FROM `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
  WHERE code_type='TRNTYP' ) cx_codes5
ON
  ( source.transaction_type_cd=cx_codes5.code
    AND cx_codes5.dupe_check = 1 )
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` membership
ON
  ( source.membership_ky=SAFE_CAST(membership.mbrs_source_system_key AS STRING)
    AND membership.actv_ind='Y' )
LEFT JOIN (
  SELECT
    MAX(batch_payment_ky) AS batch_payment_ky,
    batch_ky,
    membership_ky,
    transaction_type_cd,
    payment_method_cd
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.batch_payment`
  GROUP BY
    batch_ky,
    membership_ky,
    transaction_type_cd,
    payment_method_cd) batch_payment
ON
  (source.batch_ky=batch_payment.batch_ky
    AND source.membership_ky=batch_payment.membership_ky
    AND source.transaction_type_cd=batch_payment.transaction_type_cd
    AND source.payment_method_cd=batch_payment.payment_method_cd )
LEFT JOIN (
  SELECT
    MAX(mbrs_payment_received_adw_key) AS mbrs_payment_received_adw_key,
    payment_received_source_key,
    actv_ind
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received`
  GROUP BY
    payment_received_source_key,
    actv_ind) payments_received
ON
  SAFE_CAST(batch_payment.batch_payment_ky AS INT64)=payments_received.payment_received_source_key
  AND payments_received.actv_ind='Y'
LEFT JOIN (
  SELECT
    branch_cd,
    branch_ky,
    ROW_NUMBER() OVER(PARTITION BY branch_cd ORDER BY NULL DESC) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.branch` ) branch
ON
  source.branch_ky = branch.branch_ky
  AND branch.dupe_check = 1
LEFT JOIN (
  SELECT
    MAX(aca_office_adw_key) AS aca_office_adw_key,
    mbrs_branch_cd,
    actv_ind
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`
  GROUP BY
    mbrs_branch_cd,
    actv_ind ) aca_office
ON
  branch.branch_cd = aca_office.mbrs_branch_cd
  AND aca_office.actv_ind='Y'
LEFT JOIN (
  SELECT
    user_id,
    username,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.cx_iusers` ) cx_iusers
ON
  source.user_id = cx_iusers.user_id
  AND cx_iusers.dupe_check = 1
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` employee
ON
  ( cx_iusers.username=SAFE_CAST(employee.emp_active_directory_user_id  AS STRING)
    AND employee.actv_ind ='Y' )
WHERE
  source.dupe_check=1