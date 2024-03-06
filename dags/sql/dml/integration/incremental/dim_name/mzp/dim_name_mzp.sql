create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_name_mzp_work_source` as
select
  coalesce(TRIM(first_name),'') AS raw_first_nm,
  coalesce(TRIM(middle_name),'') AS raw_middle_nm,
  coalesce(TRIM(last_name),'') AS raw_last_nm,
  coalesce(TRIM(name_suffix),'') AS raw_suffix_nm,
  coalesce(TRIM(salutation),'') AS raw_title_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(TRIM(first_name),''), coalesce(TRIM(middle_name),''), coalesce(TRIM(last_name),''), coalesce(TRIM(name_suffix),''), coalesce(TRIM(salutation),'')) as nm_adw_key
from `{{ var.value.INGESTION_PROJECT }}.mzp.member`
union distinct
select
  coalesce(TRIM(cc_first_name),'') AS raw_first_nm,
  '' AS raw_middle_nm,
  coalesce(TRIM(cc_last_name),'') AS raw_last_nm,
  '' AS raw_suffix_nm,
  '' AS raw_title_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(TRIM(cc_first_name),''), '', coalesce(TRIM(cc_last_name),''), '', '') as nm_adw_key
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.autorenewal_card` autorenewal
union distinct
select
  coalesce(TRIM(first_name),'') AS raw_first_nm,
  '' AS raw_middle_nm,
  coalesce(TRIM(last_name),'') AS raw_last_nm,
  '' AS raw_suffix_nm,
  '' AS raw_title_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(TRIM(first_name),''), '', coalesce(TRIM(last_name),''), '', '') as nm_adw_key
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.sales_agent`
;
insert into `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name`
(nm_adw_key,
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
raw_first_nm,
raw_middle_nm,
raw_last_nm,
raw_suffix_nm,
raw_title_nm,
'verified' as  nm_valid_status_cd,
'mzp' as nm_valid_typ,
current_datetime,
{{ dag_run.id }} as  batch,
current_datetime,
{{ dag_run.id }} as  batch
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_name_mzp_work_source`
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