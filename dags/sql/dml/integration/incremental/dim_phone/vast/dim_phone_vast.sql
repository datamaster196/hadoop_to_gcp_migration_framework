create temp function phone_concat(area_code string, three string, four string, extension string) as 
( concat(coalesce(trim(area_code),''),coalesce(trim(three),''),coalesce(trim(four),''), case when length(trim(extension))>0 THEN CONCAT(' ', trim(extension),'') else '' end) )
;
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_phone_vast_work_source` as
select
phone_concat(DAY_AREA_CODE, DAY_THREE, DAY_FOUR, DAY_EXTENSION) as raw_phone_nbr,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(DAY_AREA_CODE, DAY_THREE, DAY_FOUR, DAY_EXTENSION)) as phone_adw_key
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
where length(trim(DAY_THREE))>1
UNION DISTINCT
select
phone_concat(NIGHT_AREA_CODE, NIGHT_THREE, NIGHT_FOUR, NIGHT_EXTENSION) as raw_phone_nbr,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(NIGHT_AREA_CODE, NIGHT_THREE, NIGHT_FOUR, NIGHT_EXTENSION)) as phone_adw_key
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
where length(trim(NIGHT_THREE))>1
UNION DISTINCT
select
phone_concat(OTHER_AREA_CODE, OTHER_THREE, OTHER_FOUR, OTHER_EXTENSION) as raw_phone_nbr,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(OTHER_AREA_CODE, OTHER_THREE, OTHER_FOUR, OTHER_EXTENSION)) as phone_adw_key
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
where length(trim(OTHER_THREE))>1
UNION DISTINCT
select
phone_concat(BILLPHONEAREA, BILLPHONETHREE, BILLPHONEFOUR, '') as raw_phone_nbr,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(BILLPHONEAREA, BILLPHONETHREE, BILLPHONEFOUR, '')) as phone_adw_key
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
where length(trim(BILLPHONETHREE))>1
UNION DISTINCT
select
phone_concat(FAX_AREA_CODE, FAX_THREE, FAX_FOUR, FAX_EXTENSION) as raw_phone_nbr,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(FAX_AREA_CODE, FAX_THREE, FAX_FOUR, FAX_EXTENSION)) as phone_adw_key
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
where length(trim(FAX_THREE))>1
;
insert into `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone`
(phone_adw_key,
 raw_phone_nbr,
 phone_valid_status_cd,
 phone_valid_typ,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct
phone_adw_key,
raw_phone_nbr,
'unverified' phone_valid_status_cd,
'vast' phone_valid_typ,
current_datetime,
{{ dag_run.id }} as  batch,
current_datetime,
{{ dag_run.id }} as  batch
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_phone_vast_work_source`
where phone_adw_key not in (select distinct phone_adw_key from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone`)
and raw_phone_nbr is not null
;
 -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    phone_adw_key,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone`
  GROUP BY
    1
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw_pii.dim_phone' ));