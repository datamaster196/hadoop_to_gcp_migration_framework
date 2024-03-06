INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role` ( contact_adw_key,
    ins_policy_adw_key,
    ins_rel_type,
    drvr_ins_number,
    drvr_ins_birth_dt,
    drvr_ins_good_student_cd,
    drvr_ins_good_student_desc,
    drvr_ins_marital_status_cd,
    drvr_ins_marital_status_desc,
    drvr_license_state,
    effective_start_datetime,
    effective_end_datetime,
    rel_effective_start_datetime,
    rel_effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  source.contact_adw_key,
  source.ins_policy_adw_key,
  source.ins_rel_type,
  drvr_ins_number,
  drvr_ins_birth_dt,
  drvr_ins_good_student_cd,
  drvr_ins_good_student_desc,
  drvr_ins_marital_status_cd,
  drvr_ins_marital_status_desc,
  source.drvr_license_state,
  hist_type_2.effective_start_datetime,
  hist_type_2.effective_end_datetime,
  rel_effective_start_datetime,
  rel_effective_end_datetime,
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_work_transformed` source
ON
  ( source.contact_adw_key=hist_type_2.contact_adw_key
    AND source.ins_policy_adw_key= hist_type_2.ins_policy_adw_key
    AND source.ins_rel_type= hist_type_2.ins_rel_type
    AND source.drvr_license_state = hist_type_2.drvr_license_state
    AND source.effective_start_datetime=hist_type_2.effective_start_datetime ) ;

  --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for ins_policy_adw_key
SELECT
  COUNT(target.ins_policy_adw_key ) AS ins_policy_adw_key_count
FROM (
  SELECT
    DISTINCT ins_policy_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT ins_policy_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`) source_FK_1
  WHERE
    target.ins_policy_adw_key = source_FK_1.ins_policy_adw_key)
HAVING
IF
  ((ins_policy_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.insurance_customer_role. FK Column: ins_policy_adw_key'));


   -- Orphaned foreign key check for contact_adw_key
SELECT
  COUNT(target.contact_adw_key ) AS contact_adw_key_count
FROM (
  SELECT
    DISTINCT contact_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT contact_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`) source_FK_2
  WHERE
    target.contact_adw_key = source_FK_2.contact_adw_key)
HAVING
IF
  ((contact_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.insurance_customer_role. FK Column: contact_adw_key'));


  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    contact_adw_key,
    ins_policy_adw_key,
    ins_rel_type,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role`
  GROUP BY
    1,
    2,
    3,
    4
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.insurance_customer_role' ) );
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(*)
FROM (
  SELECT
    contact_adw_key,
    ins_policy_adw_key,
    ins_rel_type,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role`) a
JOIN (
  SELECT
    contact_adw_key,
    ins_policy_adw_key,
    ins_rel_type,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role`) b
ON
  a.contact_adw_key = b.contact_adw_key
  AND a.ins_policy_adw_key = b.ins_policy_adw_key
  AND a.ins_rel_type = b.ins_rel_type
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(1) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.insurance_customer_role' ));