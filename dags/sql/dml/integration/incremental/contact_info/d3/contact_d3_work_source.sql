create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_source_work` as
select
ers.iso_cd,
ers.club_cd,
ers.membership_id,
ers.associate_id,
ers.check_digit_nr,
ers.comm_ctr_id,
ers.sc_id,
ers.sc_dt,
ers.gender,
ers.birthdate,
ers.last_upd_dt,
upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
upper(coalesce(address.cleansed_address_1_nm,address.raw_address_1_nm, '')) as cleansed_address_1_nm,
upper(coalesce(address.cleansed_city_nm,address.raw_city_nm, '')) as cleansed_city_nm,
upper(coalesce(address.cleansed_state_cd,address.raw_state_cd, '')) as cleansed_state_cd,
upper(coalesce(address.cleansed_postal_cd,address.raw_postal_cd, '')) as cleansed_postal_cd,
ers.address_adw_key,
ers.phone_adw_key,
ers.nm_adw_key
from
(
select
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(mbr_id,'iso_cd', calr_mbr_clb_cd, assoc_mbr_id) iso_cd,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(mbr_id,'club_cd', calr_mbr_clb_cd, assoc_mbr_id) club_cd,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(mbr_id,'membership_id', calr_mbr_clb_cd, assoc_mbr_id) membership_id,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(mbr_id,'associate_id', calr_mbr_clb_cd, assoc_mbr_id) associate_id,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(mbr_id,'check_digit_nr', calr_mbr_clb_cd, assoc_mbr_id) check_digit_nr,
comm_ctr_id,
sc_id,
sc_dt,
'' as gender,
cast(null as date) as birthdate,
CAST(sc_dt AS datetime) as last_upd_dt,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(trim(calr_bsc_ad),''), coalesce(trim(calr_supl_ad),''), '', coalesce(trim(calr_cty_nm),''), coalesce(trim(calr_st_cd),''), coalesce(trim(calr_zip_cd),''), '', '') as address_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(calr_fst_nm),''), '', coalesce(trim(calr_lst_nm),''), coalesce(trim(calr_suffix),''), coalesce(trim(salutation),'')) as nm_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(coalesce(trim(tel_nr_ext),'')) as phone_adw_key         
from `{{ var.value.INGESTION_PROJECT }}.d3.caller`
where 
safe_cast(mbr_id as int64) is not null AND 
upper(calr_fst_nm) not in  ('TEST','','PD') AND 
upper(calr_lst_nm) not in  ('TEST','','PD') AND 
concat(ifnull(calr_mbr_clb_cd,''),'|',ifnull(mbr_id,''),'|',ifnull(assoc_mbr_id,'')) 
         not in 
		 ( select member_identifier from
                 (select concat(ifnull(calr_mbr_clb_cd,''),'|',ifnull(mbr_id,''),'|',ifnull(assoc_mbr_id,'')) member_identifier, 
				         count(distinct concat(calr_fst_nm,calr_lst_nm)) 
						 from `{{ var.value.INGESTION_PROJECT }}.d3.caller`
						 group by 1
						 having count(distinct concat(calr_fst_nm,calr_lst_nm)) > 5
				 )
		 ) AND
CAST(sc_dt AS datetime) > (select max(effective_start_datetime) from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` where contact_source_system_nm = 'd3')
) ers
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` name on name.nm_adw_key=ers.nm_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` address on address.address_adw_key=ers.address_adw_key
