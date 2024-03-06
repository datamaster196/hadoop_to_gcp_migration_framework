INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` (nm_adw_key,
    raw_first_nm,
    raw_middle_nm,
    raw_last_nm,
    raw_suffix_nm,
    raw_title_nm,
    cleansed_first_nm,
    cleansed_middle_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_title_nm,
    nm_valid_status_cd,
    nm_valid_typ,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  DISTINCT nm_adw_key,
  firstname AS raw_first_nm,
  '' AS raw_middle_nm,
  lastname AS raw_last_nm,
  '' AS raw_suffix_nm,
  '' AS raw_title_nm,
  '' AS cleansed_first_nm,
  '' AS cleansed_middle_nm,
  '' AS cleansed_last_nm,
  '' AS cleansed_suffix_nm,
  '' AS cleansed_title_nm,
  'unverified' AS nm_valid_status_cd,
  'basic cleansing' AS nm_valid_typ,
  current_datetime AS integrate_insert_datetime,
  {{ dag_run.id }} AS integrate_insert_batch_number,
  current_datetime AS integrate_update_datetime,
  {{ dag_run.id }} AS integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed`
WHERE
  nm_adw_key NOT IN (
  SELECT
    DISTINCT nm_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name`)