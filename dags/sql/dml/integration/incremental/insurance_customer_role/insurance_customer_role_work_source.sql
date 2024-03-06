CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_customer_role_work_source` AS
SELECT
  a.uniqentity,
  b.uniqpolicy,
  contact_adw_key,
  b.ins_policy_adw_key,
  coalesce(updateddate,ts,inserteddate) as inserteddate,
  'Policy Holder' AS ins_rel_type,
  '' AS drvr_ins_number,
  CAST(NULL AS datetime) AS drvr_ins_birth_dt,
  '' AS drvr_ins_good_student_cd,
  '' AS drvr_ins_good_student_desc,
  '' AS drvr_ins_marital_status_cd,
  '' AS drvr_ins_marital_status_desc,
  '' AS drvr_license_state,
  b.effectivedate,
  expirationdate,
  ROW_NUMBER() OVER (PARTITION BY contact_adw_key, ins_policy_adw_key ORDER BY coalesce(updateddate, b.ts, inserteddate) DESC) AS dupe_check
FROM (
  SELECT
    contact_adw_key,
    uniqentity,
    uniqpolicy,
    inserteddate,
    expirationdate
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`
  JOIN
    `{{ var.value.INGESTION_PROJECT }}.epic.policy`
  ON
    uniqentity= source_1_key
  WHERE
    key_typ_nm='uniqentity_key'
    AND contact_source_system_nm='epic'
    AND source_1_key IS NOT NULL
    AND actv_ind='Y') a
JOIN (
  SELECT
    ins_policy_adw_key,
    uniqentity,
    uniqpolicy,
    ts,
    effectivedate,
    updateddate
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`
  JOIN
    `{{ var.value.INGESTION_PROJECT }}.epic.policy`
  ON
    uniqpolicy = SAFE_CAST(ins_policy_system_source_key AS string)
    and ins_policy_number =policynumber )b
ON
  a.uniqpolicy=b.uniqpolicy
WHERE
  CAST(coalesce(updateddate,
      ts,
      inserteddate) AS DATETIME) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_customer_role`) ;