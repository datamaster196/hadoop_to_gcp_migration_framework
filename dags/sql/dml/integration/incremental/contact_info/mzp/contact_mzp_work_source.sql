create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_derived_member` as 
with member_base as
(SELECT
a.MEMBER_KY,
a.MEMBERSHIP_KY,
concat(a.first_name,'|',a.last_name) as full_name,
lag(concat(a.first_name,'|',a.last_name)) over (partition by a.member_ky order by a.last_upd_dt) as prev_full_name,
case when b.member_ky is not null then 1 else 0 end as previous_diff_mem_flag,
a.LAST_UPD_DT
FROM `{{ var.value.INGESTION_PROJECT }}.mzp.member` a
left join (select membership_ky, member_ky, concat(first_name,'|',last_name) full_name, last_upd_dt FROM `{{ var.value.INGESTION_PROJECT }}.mzp.member`) b
on a.membership_ky=b.membership_ky and concat(a.first_name,'|',a.last_name)=b.full_name and b.last_upd_dt<a.last_upd_dt and a.member_ky!=b.member_ky
),
 member_prev as
 (select
member_ky,
membership_ky,
full_name,
prev_full_name,
last_upd_dt,
case when sum(previous_diff_mem_flag)>0 then 1 else 0 end as prev_diff_memb
from member_base
group by 1,2,3,4,5),
member_dates as (
select
member_ky,
membership_ky,
concat(member_ky, '_',cast(sum(1) over (partition by member_ky order by last_upd_dt asc) as string)) derived_member_ky,
last_upd_dt,
lead(last_upd_dt) over(partition by member_ky order by last_upd_dt) as next_last_upd_dt
from member_prev
where full_name!=prev_full_name and prev_diff_memb>0
)
select 
member_ky,
derived_member_ky,
cast(last_upd_dt as datetime) as effective_start_datetime,
case when next_last_upd_dt is null then cast('9999-12-31 00:00:00' as datetime) else cast(next_last_upd_dt as datetime) end as effective_end_datetime
from member_dates
;
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_source_work` AS
select
mzp.iso_cd,
mzp.club_cd,
mzp.membership_id,
mzp.associate_id,
mzp.check_digit_nr,
mzp.membership_ky,
mzp.member_ky,
mzp.derived_member_ky,
mzp.customer_id,
mzp.gender,
mzp.birthdate,
upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
upper(coalesce(address.cleansed_address_1_nm,address.raw_address_1_nm, '')) as cleansed_address_1_nm,
upper(coalesce(address.cleansed_city_nm,address.raw_city_nm, '')) as cleansed_city_nm,
upper(coalesce(address.cleansed_state_cd,address.raw_state_cd, '')) as cleansed_state_cd,
upper(coalesce(address.cleansed_postal_cd,address.raw_postal_cd, '')) as cleansed_postal_cd,
upper(coalesce(email.cleansed_email_nm,email.raw_email_nm, '')) as cleansed_email_nm,
mzp.last_upd_dt,
mzp.address_adw_key,
mzp.phone_adw_key,
mzp.nm_adw_key,
mzp.email_adw_key
from
(
SELECT
  438 as iso_cd,
  coalesce(branch.club_cd,'212') as club_cd,
  member.membership_id,
  member.associate_id,
  member.check_digit_nr,
  membership.membership_ky,
  member.member_ky,
  member.derived_member_ky,
  member.customer_id,
  coalesce(trim(member.gender),'') as gender,
  safe_cast(substr(member.birth_dt,1,10) as date) as birthdate,
  greatest(cast(member.last_upd_dt as datetime), cast(membership.last_upd_dt as datetime)) as last_upd_dt,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(TRIM(membership.address_line1),''), coalesce(TRIM(membership.address_line2),''), '', coalesce(TRIM(membership.city),''), coalesce(TRIM(membership.state),''), coalesce(TRIM(membership.zip),''), coalesce(TRIM(membership.delivery_route),''), coalesce(TRIM(membership.country),'')) as address_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(TRIM(first_name),''), coalesce(TRIM(middle_name),''), coalesce(TRIM(last_name),''), coalesce(TRIM(name_suffix),''), coalesce(TRIM(salutation),'')) as nm_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(coalesce(TRIM(membership.phone),'')) as phone_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_email`(coalesce(TRIM(email),'')) as email_adw_key   
from
    (select
      memb.*,
       row_number() over (partition by memb.member_ky order by memb.last_upd_dt desc) as latest_record_check,
       coalesce(derived_member.derived_member_ky, memb.member_ky) as derived_member_ky
      from `{{ var.value.INGESTION_PROJECT }}.mzp.member` memb
      left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_derived_member` derived_member on memb.member_ky=derived_member.member_ky and cast(memb.last_upd_dt as datetime) >= derived_member.effective_start_datetime and cast(memb.last_upd_dt as datetime) < derived_member.effective_end_datetime
	 ) member
join (select
      *,
       row_number() over (partition by membership_ky order by last_upd_dt desc) as latest_record_check
      from `{{ var.value.INGESTION_PROJECT }}.mzp.membership`) membership on member.membership_ky=membership.membership_ky and member.latest_record_check=1 and membership.latest_record_check=1 and member.status !='C'
left join (select
      *,
      row_number() over (partition by branch_ky order by last_upd_dt desc) as latest_record_check
      from `{{ var.value.INGESTION_PROJECT }}.mzp.branch`) branch on membership.branch_ky=branch.branch_ky and branch.latest_record_check=1
where greatest(cast(member.last_upd_dt as datetime), cast(membership.last_upd_dt as datetime)) > datetime_sub((select max(effective_start_datetime) from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` where contact_source_system_nm = 'mzp'), interval 12 hour)
) mzp
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` name on name.nm_adw_key=mzp.nm_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` address on address.address_adw_key=mzp.address_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` email on email.email_adw_key=mzp.email_adw_key