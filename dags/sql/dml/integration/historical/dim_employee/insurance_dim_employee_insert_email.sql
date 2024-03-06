INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` (email_adw_key,
    raw_email_nm,
    cleansed_email_nm,
    email_top_level_domain_nm,
    email_domain_nm,
    email_valid_status_cd,
    email_valid_typ,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  DISTINCT active_directory_email_adw_key,
  active_dir_email AS raw_email_nm,
  '' AS cleansed_email_nm,
  CAST(NULL AS string) email_top_level_domain_nm,
  CAST(NULL AS string) email_domain_nm,
  'unverified' email_valid_status_cd,
  'basic cleansing' email_valid_typ,
  current_datetime,
  {{ dag_run.id }} AS integrate_insert_batch_number,
  current_datetime,
  {{ dag_run.id }} AS integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed`
WHERE
  active_directory_email_adw_key NOT IN (
  SELECT
    DISTINCT email_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email`)
  AND active_dir_email IS NOT NULL
  AND active_dir_email != ''

UNION DISTINCT

SELECT
  DISTINCT email_adw_key,
  email,
  '' AS cleansed_email_nm,
  CAST(NULL AS string) email_top_level_domain_nm,
  CAST(NULL AS string) email_domain_nm,
  'unverified' email_valid_status_cd,
  'basic cleansing' email_valid_typ,
  current_datetime,
  {{ dag_run.id }} AS integrate_insert_batch_number,
  current_datetime,
  {{ dag_run.id }} AS integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed`
WHERE
  email_adw_key NOT IN (
  SELECT
    DISTINCT email_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email`)
  AND email IS NOT NULL
  AND email != ''
  