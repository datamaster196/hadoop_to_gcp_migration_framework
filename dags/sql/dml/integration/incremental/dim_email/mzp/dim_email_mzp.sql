create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_email_mzp_work_source` as
select
distinct
    coalesce(TRIM(email),'') AS raw_email_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_email`(coalesce(TRIM(email),'')) as email_adw_key
from `{{ var.value.INGESTION_PROJECT }}.mzp.member`
;
insert into `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email`
(email_adw_key,
 raw_email_nm,
 email_valid_status_cd,
 email_valid_typ,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct 
email_adw_key,
raw_email_nm,
'unverified' email_valid_status_cd,
'mzp' email_valid_typ,
current_datetime,
{{ dag_run.id }} as  batch,
current_datetime,
{{ dag_run.id }} as  batch
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_email_mzp_work_source`
where email_adw_key not in (select distinct email_adw_key from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email`)
;
 -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    email_adw_key,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email`
  GROUP BY
    1
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw_pii.dim_email' ));