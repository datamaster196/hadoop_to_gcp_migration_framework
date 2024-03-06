CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_work_stage` AS
SELECT
  source.contact_adw_key,
  source.ins_policy_adw_key,
  source.ins_rel_type,
  source.drvr_ins_number,
  source.drvr_ins_birth_dt,
  source.drvr_ins_good_student_cd,
  source.drvr_ins_good_student_desc,
  source.drvr_ins_marital_status_cd,
  source.drvr_ins_marital_status_desc,
  source.drvr_license_state,
  source.rel_effective_start_datetime,
  source.rel_effective_end_datetime,
  source.effective_start_datetime,
  CAST('9999-12-31' AS datetime) AS effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} as integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role` target
ON
  ( source.contact_adw_key=target.contact_adw_key
    AND source.ins_policy_adw_key= target.ins_policy_adw_key
    AND source.ins_rel_type= target.ins_rel_type
    AND target.actv_ind='Y' )
WHERE
  (target.contact_adw_key IS NULL
    AND target.ins_policy_adw_key IS NULL
    AND target.ins_rel_type IS NULL)
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.contact_adw_key,
  target.ins_policy_adw_key,
  target.ins_rel_type,
  target.drvr_ins_number,
  target.drvr_ins_birth_dt,
  target.drvr_ins_good_student_cd,
  target.drvr_ins_good_student_desc,
  target.drvr_ins_marital_status_cd,
  target.drvr_ins_marital_status_desc,
  target.drvr_license_state,
  target.rel_effective_start_datetime,
  target.rel_effective_end_datetime,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 second) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  target.integrate_update_batch_number AS integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role` target
ON
  ( source.contact_adw_key=target.contact_adw_key
    AND source.ins_policy_adw_key= target.ins_policy_adw_key
    AND source.ins_rel_type= target.ins_rel_type
    AND target.actv_ind='Y' )
WHERE
  source.adw_row_hash <> target.adw_row_hash ;