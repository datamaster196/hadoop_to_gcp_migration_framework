merge into `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` target
using
(
select 
customer.contact_adw_key,
coalesce(name.cleansed_first_nm,name.raw_first_nm) first_nm,
coalesce(name.cleansed_middle_nm,name.raw_middle_nm) middle_nm,
coalesce(name.cleansed_last_nm,name.raw_last_nm) last_nm,
coalesce(name.cleansed_suffix_nm, name.raw_suffix_nm) suffix_nm,
coalesce(name.cleansed_title_nm, name.raw_title_nm) title_nm,
coalesce(address.cleansed_address_1_nm, address.raw_address_1_nm)  address_1_nm,
coalesce(address.cleansed_city_nm, address.raw_city_nm) city_nm,
coalesce(address.cleansed_state_cd, address.raw_state_cd) state_cd,
coalesce(address.cleansed_postal_cd, address.raw_postal_cd) postal_cd,
coalesce(address.cleansed_postal_plus_4_cd, address.raw_postal_plus_4_cd) postal_plus_4_cd,
coalesce(address.cleansed_country_nm, address.raw_country_nm) country_nm,
phone.raw_phone_nbr phone_nbr,
coalesce(phone.cleansed_ph_nbr, phone.raw_phone_nbr) phone_formatted_nbr,
phone.cleansed_ph_extension_nbr ph_extension_nbr,
coalesce(email.cleansed_email_nm, email.raw_email_nm) email_nm,
customer.contact_birth_dt,
primacy.contact_source_system_nm as primacy_source_system_nm,
(TO_BASE64(MD5(CONCAT(ifnull(coalesce(name.cleansed_first_nm,name.raw_first_nm),''),'|'
                     ,ifnull(coalesce(name.cleansed_middle_nm,name.raw_middle_nm),''),'|'
                     ,ifnull(coalesce(name.cleansed_last_nm,name.raw_last_nm) ,''),'|'                
                     ,ifnull(coalesce(name.cleansed_suffix_nm, name.raw_suffix_nm),''),'|'
                     ,ifnull(coalesce(name.cleansed_title_nm, name.raw_title_nm),''),'|'                                     
                     ,ifnull(coalesce(address.cleansed_address_1_nm, address.raw_address_1_nm),''),'|'
                     ,ifnull(coalesce(address.cleansed_city_nm, address.raw_city_nm),''),'|'                                     
                     ,ifnull(coalesce(address.cleansed_state_cd, address.raw_state_cd),''),'|'                                 
                     ,ifnull(coalesce(address.cleansed_postal_cd, address.raw_postal_cd),''),'|'
                     ,ifnull(coalesce(address.cleansed_postal_plus_4_cd, address.raw_postal_plus_4_cd),''),'|'                                     
                     ,ifnull(coalesce(address.cleansed_country_nm, address.raw_country_nm),''),'|'
                     ,ifnull(phone.raw_phone_nbr,''),'|'                                     
                     ,ifnull(coalesce(phone.cleansed_ph_nbr, phone.raw_phone_nbr),''),'|'
                     ,ifnull(cast(phone.cleansed_ph_extension_nbr as string),''),'|'                                     
                     ,ifnull(coalesce(email.cleansed_email_nm, email.raw_email_nm),''),'|'
                     ,ifnull(safe_cast(customer.contact_birth_dt as string),''), '|'
                     ,ifnull(primacy.contact_source_system_nm,'')					 
                      )))) as adw_row_hash
from
`{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` customer
join 
(select
contact_adw_key,
contact_source_system_nm,
row_number() over(partition by contact_adw_key order by case when contact_source_system_nm='mzp' and effective_end_datetime=datetime('9999-12-31') then 1 
                                                             else 2 end asc, 
                                                        effective_end_datetime desc, 
                                                        effective_start_datetime desc) as rn
from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`
) primacy on customer.contact_adw_key=primacy.contact_adw_key    and primacy.rn=1

left outer join (select *,
                        case when contact_source_system_nm='mzp' then 1 else 0 end as mzp_flag,
                        row_number() over(partition by contact_adw_key, contact_source_system_nm order by effective_start_datetime desc, nm_adw_key) as rn
						from `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name`
                        where effective_end_datetime=datetime('9999-12-31')
				) name_bridge 
    on customer.contact_adw_key=name_bridge.contact_adw_key and
    name_bridge.contact_source_system_nm=primacy.contact_source_system_nm and 
	name_bridge.rn=1
    

left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` name 
    on name_bridge.nm_adw_key=name.nm_adw_key

left outer join (select *,
                        case when contact_source_system_nm='mzp' then 1 else 0 end as mzp_flag,
                        row_number() over(partition by contact_adw_key, contact_source_system_nm order by effective_start_datetime desc, address_adw_key) as rn
						from `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address`
                        where effective_end_datetime=datetime('9999-12-31')
				) address_bridge 
    on customer.contact_adw_key=address_bridge.contact_adw_key and
    address_bridge.contact_source_system_nm=primacy.contact_source_system_nm and 
	address_bridge.rn=1

left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` address 
    on address_bridge.address_adw_key=address.address_adw_key

left outer join (select *,
                        case when contact_source_system_nm='mzp' then 1 else 0 end as mzp_flag,
                        row_number() over(partition by contact_adw_key, contact_source_system_nm order by effective_start_datetime desc, email_adw_key) as rn
						from `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_email`
                        where effective_end_datetime=datetime('9999-12-31')
				) email_bridge 
    on customer.contact_adw_key=email_bridge.contact_adw_key and
    email_bridge.contact_source_system_nm=primacy.contact_source_system_nm and
    email_bridge.rn=1

left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` email 
    on email_bridge.email_adw_key=email.email_adw_key

left outer join (select *,
                        case when contact_source_system_nm='mzp' then 1 else 0 end as mzp_flag,
                        row_number() over(partition by contact_adw_key, contact_source_system_nm order by effective_start_datetime desc, phone_adw_key) as rn
						from `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_phone`
                        where effective_end_datetime=datetime('9999-12-31')
				) phone_bridge 
    on customer.contact_adw_key=phone_bridge.contact_adw_key and
    phone_bridge.contact_source_system_nm=primacy.contact_source_system_nm and
    phone_bridge.rn=1

left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone` phone 
    on phone_bridge.phone_adw_key=phone.phone_adw_key

) source
on source.contact_adw_key=target.contact_adw_key
when matched and source.adw_row_hash<>target.adw_row_hash then update
set 
target.contact_first_nm=source.first_nm,
target.contact_middle_nm=source.middle_nm,
target.contact_last_nm=source.last_nm,
target.contact_suffix_nm=source.suffix_nm,
target.contact_title_nm=source.title_nm,
target.contact_address_1_nm=source.address_1_nm,
target.contact_city_nm=source.city_nm,
target.contact_state_cd=source.state_cd,
target.contact_postal_cd=source.postal_cd,
target.contact_postal_plus_4_cd=source.postal_plus_4_cd,
target.contact_country_nm=source.country_nm,
target.contact_email_nm=source.email_nm,
target.contact_phone_nbr=source.phone_nbr,
target.contact_phone_formatted_nbr=source.phone_formatted_nbr,
target.contact_phone_extension_nbr=source.ph_extension_nbr,
target.primacy_source_system_nm=source.primacy_source_system_nm,
target.adw_row_hash=source.adw_row_hash,
target.integrate_update_datetime=current_datetime,
target.integrate_update_batch_number={{ dag_run.id }}
