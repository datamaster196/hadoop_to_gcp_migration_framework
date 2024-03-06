CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_work_transformed` AS
SELECT
  contact_adw_key,
  ins_policy_adw_key,
  ins_rel_type,
  drvr_ins_number,
  drvr_ins_birth_dt,
  drvr_ins_good_student_cd,
  drvr_ins_good_student_desc,
  drvr_ins_marital_status_cd,
  drvr_ins_marital_status_desc,
  drvr_license_state,
  SAFE_CAST(effectivedate as DATETIME) as rel_effective_start_datetime,
  SAFE_CAST(expirationdate as DATETIME) as rel_effective_end_datetime,
  CAST(inserteddate AS datetime) AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT(ifnull(COALESCE(contact_adw_key,
            '-1'),
          ''),'|',ifnull(COALESCE(ins_policy_adw_key,
            '-1'),
          ''),'|',ifnull(ins_rel_type,
          ''),'|',ifnull(drvr_ins_number,
          ''),'|',ifnull(SAFE_CAST(drvr_ins_birth_dt AS string),
          ''),'|',ifnull(drvr_ins_good_student_cd,
          ''),'|',ifnull(drvr_ins_good_student_desc,
          ''),'|',ifnull(drvr_ins_marital_status_cd,
          ''),'|',ifnull(drvr_ins_marital_status_desc,
          ''),'|',ifnull(drvr_license_state,
          ''),'|',ifnull(effectivedate,
          ''),'|',ifnull(expirationdate,
          '')))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_work_source`
  where dupe_check=1;