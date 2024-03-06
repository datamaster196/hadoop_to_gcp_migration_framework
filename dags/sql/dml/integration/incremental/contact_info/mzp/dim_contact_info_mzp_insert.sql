INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` (contact_adw_key,
    contact_gender_cd,
    contact_gender_nm,
    contact_birth_dt,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  contact_adw_key,
  MAX(gender) AS contact_gender_cd,
  CAST(NULL AS string) AS contact_gender_nm,
  MAX(birthdate) AS birth_date,
  '-1' AS adw_row_hash,
  --default initial insert
  current_datetime,
  {{ dag_run.id }} AS batch_insert,
  current_datetime,
  {{ dag_run.id }} AS batch_update
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_stage`
WHERE
  contact_adw_key NOT IN (
  SELECT
    DISTINCT contact_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`)
GROUP BY
  contact_adw_key ;

    
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
-- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    contact_adw_key, 
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`
  GROUP BY
    1
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw_pii.dim_contact_info' )) 
