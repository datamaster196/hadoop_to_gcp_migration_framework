CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_employee_source` AS
SELECT
  'E' AS emp_typ_cd,
  cx_iusers.username,
  email AS email,
  '' AS emp_hr_id ,
  '' AS emp_hr_hire_dt,
  '' AS emp_hr_term_dt,
  '' AS emp_hr_term_reason,
  '' AS emp_hr_status,
  '' AS emp_hr_region,
  '' AS emp_loc_hr,
  '' AS emp_hr_title,
  '' AS emp_hr_sprvsr,
  '-1' AS fax_phone_adw_key,
  '-1' AS phone_adw_key,
  '-1' AS nm_adw_key,
  '-1' AS email_adw_key,
  '' AS emp_hr_job_cd,
  '' AS emp_hr_cost_center,
  '' AS emp_hr_position_typ,
  ROW_NUMBER() OVER (PARTITION BY cx_iusers.username ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.cx_iusers` cx_iusers
WHERE
  CAST(cx_iusers.adw_lake_insert_datetime AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee`)



