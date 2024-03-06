CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.epic_contact_target_work` AS
SELECT
  epic_keys.contact_adw_key AS contact_adw_key,
  epic_keys.source_1_key AS client_unique_entity,
'' as iso_cd,
'' as club_cd,
'' as membership_id,
'' as associate_id,
'' as check_digit_nr, 
'' as cleansed_first_nm,
'' as cleansed_last_nm,
'' as cleansed_suffix_nm,
'' as cleansed_address_1_nm,
'' as cleansed_city_nm,
'' as cleansed_state_cd,
'' as cleansed_postal_cd,
''  AS cleansed_email_nm,
cast(null as datetime) as effective_start_datetime,
cast(null as datetime) as effective_end_datetime,
0 as mbr_id_count
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` epic_keys
where epic_keys.contact_source_system_nm='epic'
  AND epic_keys.key_typ_nm='uniqentity_key'
  
union all
--Membership ID records
select
member_ids.contact_adw_key as contact_adw_key,
'' AS client_unique_entity,
member_ids.source_1_key as iso_cd,
member_ids.source_2_key as club_cd,
member_ids.source_3_key as membership_id,
member_ids.source_4_key as associate_id,
member_ids.source_5_key as check_digit_nr, 

'' as cleansed_first_nm,
'' as cleansed_last_nm,
'' as cleansed_suffix_nm,
'' as cleansed_address_1_nm,
'' as cleansed_city_nm,
'' as cleansed_state_cd,
'' as cleansed_postal_cd,
''  AS cleansed_email_nm,

member_ids.effective_start_datetime as effective_start_datetime,
member_ids.effective_end_datetime as effective_end_datetime,

member_ids.mbr_id_count as mbr_id_count
from (select  contact_adw_key, 
                   source_1_key,
                   source_2_key,
				   source_3_key,
				   source_4_key,
				   source_5_key,
				   count(distinct contact_adw_key) over(partition by source_2_key, source_3_key, source_4_key) as mbr_id_count,
				   effective_start_datetime,
				   effective_end_datetime
		  from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` 
		  where key_typ_nm like 'member_id%' -- all member ids regardless of source
		        and length(source_3_key) in (7,8)
		  ) member_ids

union all
--Name address records
select
contact.contact_adw_key as contact_adw_key,

'' AS client_unique_entity,

'' as iso_cd,
'' as club_cd,
'' as membership_id,
'' as associate_id,
'' as check_digit_nr, 

upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
upper(coalesce(address.cleansed_address_1_nm,address.raw_address_1_nm, '')) as cleansed_address_1_nm,
upper(coalesce(address.cleansed_city_nm,address.raw_city_nm, '')) as cleansed_city_nm,
upper(coalesce(address.cleansed_state_cd,address.raw_state_cd, '')) as cleansed_state_cd,
upper(coalesce(address.cleansed_postal_cd,address.raw_postal_cd, '')) as cleansed_postal_cd,
''  AS cleansed_email_nm,

cast(null as datetime) as effective_start_datetime,
cast(null as datetime) as effective_end_datetime,
0 as mbr_id_count
from
`{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` contact
inner join (select distinct contact_adw_key, 
                  cleansed_first_nm, 
                  cleansed_last_nm, 
                  cleansed_suffix_nm,
				  raw_first_nm,
				  raw_last_nm,
				  raw_suffix_nm
           from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name` name_bridge 
           join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` dim_name
           on name_bridge.nm_adw_key=dim_name.nm_adw_key
           ) name
           on contact.contact_adw_key=name.contact_adw_key


inner join (select distinct contact_adw_key, 
                  cleansed_address_1_nm,
                  cleansed_city_nm,
                  cleansed_state_cd,
                  cleansed_postal_cd,
                  raw_address_1_nm,
                  raw_city_nm,
                  raw_state_cd,
                  raw_postal_cd
           from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address` address_bridge 
           join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` dim_address
           on address_bridge.address_adw_key=dim_address.address_adw_key
           ) address
           on contact.contact_adw_key=address.contact_adw_key
union all
--Name Email records
select
contact.contact_adw_key as contact_adw_key,

'' AS client_unique_entity,

'' as iso_cd,
'' as club_cd,
'' as membership_id,
'' as associate_id,
'' as check_digit_nr, 

upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
'' as cleansed_address_1_nm,
'' as cleansed_city_nm,
'' as cleansed_state_cd,
'' as cleansed_postal_cd,
upper(coalesce(email.cleansed_email_nm ,email.raw_email_nm , ''))  AS cleansed_email_nm,

cast(null as datetime) as effective_start_datetime,
cast(null as datetime) as effective_end_datetime,
0 as mbr_id_count
from
`{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` contact
inner join (select distinct contact_adw_key, 
                  cleansed_first_nm, 
                  cleansed_last_nm, 
                  cleansed_suffix_nm,
				  raw_first_nm,
				  raw_last_nm,
				  raw_suffix_nm
           from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name` name_bridge 
           join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` dim_name
           on name_bridge.nm_adw_key=dim_name.nm_adw_key
           ) name
           on contact.contact_adw_key=name.contact_adw_key


inner join (select distinct contact_adw_key, 
                  cleansed_email_nm,
				  raw_email_nm
           from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_email` email_bridge 
           join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` dim_email
           on email_bridge.email_adw_key=dim_email.email_adw_key
           ) email
           on contact.contact_adw_key=email.contact_adw_key