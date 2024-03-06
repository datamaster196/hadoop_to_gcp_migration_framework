create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_name_vast_work_source` as
select
distinct
coalesce(trim(FIRST_NAME),'') as raw_first_nm,
'' as raw_middle_nm,
coalesce(trim(LAST_NAME),'') as raw_last_nm,
'' as raw_suffix_nm,
'' as raw_title_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(FIRST_NAME),''), '', coalesce(trim(LAST_NAME),''), '', '') as nm_adw_key
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
;
insert into `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name`
(nm_adw_key,
 raw_first_nm,
 raw_middle_nm,
 raw_last_nm,
 raw_suffix_nm,
 raw_title_nm,
 nm_valid_status_cd,
 nm_valid_typ,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct
nm_adw_key ,
raw_first_nm,
raw_middle_nm,
raw_last_nm,
raw_suffix_nm,
raw_title_nm,
'unverified' as  nm_valid_status_cd,
'vast' as nm_valid_typ,
current_datetime,
{{ dag_run.id }} as  batch,
current_datetime,
{{ dag_run.id }} as  batch
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_name_vast_work_source`
where nm_adw_key not in (select distinct nm_adw_key from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name`)
;
 -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    nm_adw_key,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name`
  GROUP BY
    1
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw_pii.dim_name' ));