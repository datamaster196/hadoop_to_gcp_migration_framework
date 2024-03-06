create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_address_mzp_work_source` as
select
  coalesce(TRIM(address_line1),'') AS raw_address_1_nm,
  coalesce(TRIM(address_line2),'') AS raw_address_2_nm,
  CAST('' AS string) AS raw_care_of_nm,
  coalesce(TRIM(city),'') AS raw_city_nm,
  coalesce(TRIM(state),'') AS raw_state_cd,
  coalesce(TRIM(zip),'') AS raw_postal_cd,
  coalesce(TRIM(delivery_route),'') AS raw_postal_plus_4_cd,
  coalesce(TRIM(country),'') AS raw_country_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(TRIM(address_line1),''), coalesce(TRIM(address_line2),''), '', coalesce(TRIM(city),''), coalesce(TRIM(state),''), coalesce(TRIM(zip),''), coalesce(TRIM(delivery_route),''), coalesce(TRIM(country),'')) as address_adw_key
from `{{ var.value.INGESTION_PROJECT }}.mzp.membership`
union distinct
select
  coalesce(TRIM(CC_STREET),'') AS raw_address_1_nm,
  CAST('' AS string) AS raw_address_2_nm,
  CAST('' AS string) AS raw_care_of_nm,
  coalesce(TRIM(CC_City),'') AS raw_city_nm,
  coalesce(TRIM(CC_State),'') AS raw_state_cd,
  coalesce(TRIM(CC_Zip),'') AS raw_postal_cd,
  CAST('' AS string) AS raw_postal_plus_4_cd,
  CAST('' AS string) AS raw_country_nm,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(TRIM(CC_STREET),''), '', '', coalesce(TRIM(CC_City),''), coalesce(TRIM(CC_State),''), coalesce(TRIM(CC_Zip),''), '', '') as address_adw_key
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.autorenewal_card`
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
 cleansed_address_1_nm,
 cleansed_address_2_nm,
 cleansed_care_of_nm,
 cleansed_city_nm,
 cleansed_state_cd,
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
raw_address_1_nm,
raw_address_2_nm,
raw_care_of_nm,
raw_city_nm,
raw_state_cd,
raw_postal_cd,
raw_postal_plus_4_cd,
raw_country_nm,
'verified' address_valid_status_cd,
'mzp' address_valid_typ,
current_datetime,
{{ dag_run.id }} as  batch,
current_datetime,
{{ dag_run.id }} as  batch
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_address_mzp_work_source`
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