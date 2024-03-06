create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_name_d3_work_source` as
select
distinct
coalesce(trim(calr_fst_nm),'') as raw_first_nm,
'' as raw_middle_nm,
coalesce(trim(calr_lst_nm),'') as raw_last_nm,
coalesce(trim(calr_suffix),'') as raw_suffix_nm,
coalesce(trim(salutation),'') as raw_title_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(calr_fst_nm),''), '', coalesce(trim(calr_lst_nm),''), coalesce(trim(calr_suffix),''), coalesce(trim(salutation),'')) as nm_adw_key
from `{{ var.value.INGESTION_PROJECT }}.d3.caller`
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
'd3' as nm_valid_typ,
current_datetime,
{{ dag_run.id }} as  batch,
current_datetime,
{{ dag_run.id }} as  batch
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_name_d3_work_source`
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