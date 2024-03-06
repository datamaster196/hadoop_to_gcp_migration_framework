create temp function phone_concat(area_code string, three string, four string, extension string) as 
( concat(coalesce(trim(area_code),''),coalesce(trim(three),''),coalesce(trim(four),''), case when length(trim(extension))>0 THEN CONCAT(' ', trim(extension),'') else '' end) )
;
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vast_contact_source_work` as
select
vast.iso_cd,
vast.club_cd,
vast.membership_id,
vast.associate_id,
vast.check_digit_nr,
vast.vast_cust_nbr,
vast.company,
vast.gender,
vast.birthdate,
vast.last_upd_dt,
upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
upper(coalesce(address.cleansed_address_1_nm,address.raw_address_1_nm, '')) as cleansed_address_1_nm,
upper(coalesce(address.cleansed_city_nm,address.raw_city_nm, '')) as cleansed_city_nm,
upper(coalesce(address.cleansed_state_cd,address.raw_state_cd, '')) as cleansed_state_cd,
upper(coalesce(address.cleansed_postal_cd,address.raw_postal_cd, '')) as cleansed_postal_cd,
upper(coalesce(email.cleansed_email_nm,email.raw_email_nm, '')) as cleansed_email_nm,
vast.address_adw_key,
vast.day_phone_adw_key,
vast.night_phone_adw_key,
vast.other_phone_adw_key,
vast.bill_phone_adw_key,
vast.fax_phone_adw_key,
vast.nm_adw_key,
vast.email_adw_key
from
(
select
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(Member_ID_Number,'iso_cd', Member_ID_Number, Member_ID_Number) iso_cd,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(Member_ID_Number,'club_cd', Member_ID_Number, Member_ID_Number) club_cd,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(Member_ID_Number,'membership_id', Member_ID_Number, Member_ID_Number) membership_id,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(Member_ID_Number,'associate_id', Member_ID_Number, Member_ID_Number) associate_id,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(Member_ID_Number,'check_digit_nr', Member_ID_Number, Member_ID_Number) check_digit_nr,
customer_number as vast_cust_nbr,
ifnull(company, '-1') as company,
GENDER as gender,
cast(null as date) as birthdate,
CAST(LASTUPDATEDATE AS datetime) as last_upd_dt,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(trim(ADDRESS),''), coalesce(trim(ADDRESS2),''), '', coalesce(trim(CITY),''), coalesce(trim(STATE),''), coalesce(trim(ZIPCODE),''), '', '') as address_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(FIRST_NAME),''), '', coalesce(trim(LAST_NAME),''), '', '') as nm_adw_key,
case when length(trim(DAY_THREE))> 1 
     then `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(DAY_AREA_CODE, DAY_THREE, DAY_FOUR, DAY_EXTENSION)) 
	 else '-1' end as day_phone_adw_key,
case when length(trim(NIGHT_THREE))> 1 
     then `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(NIGHT_AREA_CODE, NIGHT_THREE, NIGHT_FOUR, NIGHT_EXTENSION)) 
	 else '-1' end as night_phone_adw_key,
case when length(trim(OTHER_THREE))> 1 
     then `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(OTHER_AREA_CODE, OTHER_THREE, OTHER_FOUR, OTHER_EXTENSION)) 
	 else '-1' end as other_phone_adw_key,
case when length(trim(BILLPHONETHREE))> 1 
     then `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(BILLPHONEAREA, BILLPHONETHREE, BILLPHONEFOUR, '')) 
	 else '-1' end as bill_phone_adw_key,
case when length(trim(FAX_THREE))> 1 
     then `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(FAX_AREA_CODE, FAX_THREE, FAX_FOUR, FAX_EXTENSION)) 
	 else '-1' end as fax_phone_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_email`(coalesce(trim(EMAILADDRESS),'')) as email_adw_key,         
from `{{ var.value.INGESTION_PROJECT }}.vast.customer`
where 
CAST(LASTUPDATEDATE AS datetime) > (select max(effective_start_datetime) from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` where contact_source_system_nm = 'vast')
) vast
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` name on name.nm_adw_key=vast.nm_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` address on address.address_adw_key=vast.address_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` email on email.email_adw_key=vast.email_adw_key
