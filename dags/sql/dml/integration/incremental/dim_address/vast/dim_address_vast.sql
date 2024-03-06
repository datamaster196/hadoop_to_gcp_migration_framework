create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_address_vast_work_source` as
select
distinct
coalesce(trim(ADDRESS),'') as raw_address_1_nm,
coalesce(trim(ADDRESS2),'') as raw_address_2_nm,
cast('' as string) as raw_care_of_nm,
coalesce(trim(CITY),'') as raw_city_nm,
coalesce(trim(STATE),'') as raw_state_cd,
coalesce(trim(ZIPCODE),'') as raw_postal_cd,
'' as raw_postal_plus_4_cd,
'' as raw_country_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(trim(ADDRESS),''), coalesce(trim(ADDRESS2),''), '', coalesce(trim(CITY),''), coalesce(trim(STATE),''), coalesce(trim(ZIPCODE),''), '', '') as address_adw_key
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
;
insert into `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address`
(address_adw_key,
 raw_address_1_nm,
 raw_address_2_nm,
 raw_care_of_nm,
 raw_city_nm,
 raw_state_cd,
 raw_postal_cd,
 raw_postal_plus_4_cd,
 raw_country_nm,
 cleansed_state_cd,
 cleansed_state_nm,
 cleansed_postal_cd,
 cleansed_postal_plus_4_cd,
 cleansed_country_nm,
 address_valid_status_cd,
 address_valid_typ,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct 
address_adw_key,
raw_address_1_nm,
raw_address_2_nm,
raw_care_of_nm,
raw_city_nm,
raw_state_cd,
raw_postal_cd,
raw_postal_plus_4_cd,
raw_country_nm,
ifnull(state_cd,'UN') as cleansed_state_cd,
state_nm cleansed_state_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_zipcode`(raw_postal_cd,raw_postal_plus_4_cd, country_cd, 'prefix') cleansed_postal_cd,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_zipcode`(raw_postal_cd,raw_postal_plus_4_cd, country_cd, 'suffix') cleansed_postal_plus_4_cd,
country_cd cleansed_country_nm,
'unverified' address_valid_status_cd,
'vast' address_valid_typ,
current_datetime,
{{ dag_run.id }} as  batch,
current_datetime,
{{ dag_run.id }} as  batch
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_address_vast_work_source`
left join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_state` on upper(raw_state_cd)=state_cd 
where address_adw_key not in (select distinct address_adw_key from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address`)
;
 -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    address_adw_key,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address`
  GROUP BY
    1
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw_pii.dim_address' ));