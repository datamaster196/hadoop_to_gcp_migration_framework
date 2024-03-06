create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.market_magnifier_contact_source_work` as
select
mm.iso_cd,
mm.club_cd,
mm.membership_id,
mm.associate_id,
mm.check_digit_nr,
mm.lexid,
mm.lexhhid,
mm.gender,
mm.birthdate,
mm.last_upd_dt,
upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
upper(coalesce(address.cleansed_address_1_nm,address.raw_address_1_nm, '')) as cleansed_address_1_nm,
upper(coalesce(address.cleansed_city_nm,address.raw_city_nm, '')) as cleansed_city_nm,
upper(coalesce(address.cleansed_state_cd,address.raw_state_cd, '')) as cleansed_state_cd,
upper(coalesce(address.cleansed_postal_cd,address.raw_postal_cd, '')) as cleansed_postal_cd,
mm.address_adw_key,
mm.phone_adw_key,
mm.nm_adw_key
from
(
select
  NULL AS iso_cd,
  case when length(memb_id)=7 then club else null end AS club_cd,
  case when length(memb_id)=7 then memb_id else null end AS membership_id,
  case when length(memb_id)=7 then ass_id else null end AS associate_id,
  NULL AS check_digit_nr,
  lexid,
  lexhhid,
  coalesce(TRIM(gender_code_drv),'') as gender,
  parse_DATE('%Y%m%d',
    CASE
      WHEN LENGTH(exact_dob_drv_orig)=10 THEN exact_dob_drv_orig
    ELSE
    NULL
  END
    ) AS birthdate,
  datetime_TRUNC(adw_lake_insert_datetime, month) as last_upd_dt,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(trim(ace_prim_addr),''), coalesce(trim(ace_sec_addr),''), '', coalesce(trim(City),''), coalesce(trim(State),''), coalesce(trim(zip),''), coalesce(trim(zip4),''), '') as address_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(first_name),''), coalesce(trim(middle_initial),''), coalesce(trim(last_name),''), coalesce(trim(surname_suffix_p1),''), coalesce(trim(Title_of_Respect_P1),'')) as nm_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(coalesce(trim(phone_drv),'')) as phone_adw_key         
from `{{ var.value.INGESTION_PROJECT }}.demographic.marketing_magnifier`
WHERE
  LENGTH(TRIM(lexid))>0
  AND datetime_TRUNC(adw_lake_insert_datetime,
    month) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`
  WHERE
    contact_source_system_nm = 'market_magnifier')
) mm
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` name on name.nm_adw_key=mm.nm_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` address on address.address_adw_key=mm.address_adw_key
