create temp function phone_concat(area_code string, tel_nr string, extension string) as 
( concat(coalesce(trim(area_code),''),coalesce(trim(tel_nr),''), case when length(trim(extension))>0 THEN CONCAT(' ', trim(extension),'') else '' end) )
;
create temp function decode_salutation(salutation string) as 
( case when salutation='100' then 'DR'
       when salutation='101' then 'MISS'
       when salutation='102' then 'MR'
       when salutation='103' then 'SIST'
       when salutation='104' then 'MS'
       when salutation='105' then 'PROF'
       when salutation='106' then 'REV'
       when salutation='107' then 'MRS'
       else salutation end
 )
;
create temp function decode_cust_adr(cust_adr_id string) as 
( case when cust_adr_id='25' then 'business'
       when cust_adr_id='26' then 'home'
       when cust_adr_id='488' then 'shipping'
       when cust_adr_id='845' then 'customer_ident_adr'
       else cust_adr_id end
 )
;
create temp function decode_cust_tel(cust_tel_id string) as 
( case when cust_tel_id='108' then 'business'
       when cust_tel_id='109' then 'charge_phone'
       when cust_tel_id='110' then 'fax'
       when cust_tel_id='111' then 'modem'
       when cust_tel_id='112' then 'pay'
       when cust_tel_id='113' then 'pager'
       when cust_tel_id='114' then 'primary_business'
       when cust_tel_id='115' then 'residence'
       when cust_tel_id='116' then 'secondary_business'
       when cust_tel_id='117' then 'cell'
       else cust_tel_id end
 )
;
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_source_work` as
select
cust_to_parse,
case when length(cust_to_parse)=16 then substr(cust_to_parse,1,3) else null end as iso_cd,
case when length(cust_to_parse)=16 then substr(cust_to_parse,4,3)
     when length(cust_to_parse) in (11,12) then substr(cust_to_parse,1,3) 
     else null end as club_cd,
case when length(cust_to_parse)=16 then substr(cust_to_parse,7,7)
     when length(cust_to_parse) = 11 and substr(cust_to_parse,1,3) = '007' then substr(cust_to_parse,4,8)  
     when length(cust_to_parse) in (11,12) then substr(cust_to_parse,4,7)
     else null end as membership_id,
case when length(cust_to_parse)=16 then substr(cust_to_parse,14,2)
     when length(cust_to_parse)=12 then substr(cust_to_parse,11,2)
     when length(cust_to_parse) = 11 and substr(cust_to_parse,1,3) = '007' then substr(cust_to_parse,15,1)  
     when length(cust_to_parse)=11 then lpad(substr(cust_to_parse,11,1),2,'0')
     else null end as associate_id,
case when length(cust_to_parse)=16 then substr(cust_to_parse,16,1) else null end as check_digit_nr,
pos.pos_cust_id,
pos.gender,
pos.birthdate,
pos.last_upd_dt,
upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
upper(coalesce(address.cleansed_address_1_nm,address.raw_address_1_nm, '')) as cleansed_address_1_nm,
upper(coalesce(address.cleansed_city_nm,address.raw_city_nm, '')) as cleansed_city_nm,
upper(coalesce(address.cleansed_state_cd,address.raw_state_cd, '')) as cleansed_state_cd,
upper(coalesce(address.cleansed_postal_cd,address.raw_postal_cd, '')) as cleansed_postal_cd,
pos.address_typ_cd,
pos.address_adw_key,
pos.phone_typ_cd,
pos.phone_adw_key,
pos.nm_adw_key
from
(
select
cust.cust_id as pos_cust_id,
case when substr(cust.cust_id,1,1)!='M' then ''
     when substr(cust.cust_id,2,3)='212' then substr(cust.cust_id,2,12)
     when strpos(cust.cust_id,'-')>0 then substr(cust.cust_id,2,strpos(cust.cust_id,'-')-2)
when length(trim(cust.cust_id))=12 then substr(cust.cust_id,2,11)
when length(trim(cust.cust_id)) in (13,14) then substr(cust.cust_id,2,12)
     when length(trim(cust.cust_id))=16 then substr(cust.cust_id,5,12)
     when length(trim(cust.cust_id))=15 then substr(cust.cust_id,2,11)
     when length(trim(cust.cust_id))=20 then substr(cust.cust_id,5,16)
     when length(trim(cust.cust_id))< 12 then substr(cust.cust_id,2,length(trim(cust.cust_id))-1)   
     end as cust_to_parse,
decode_cust_tel(cust_tel_typ_id) as phone_typ_cd,
decode_cust_adr(cust_adr_typ_id) as address_typ_cd,
cust.clb_cd,
'' as gender,
safe_cast(substr(cust_dob,1,10) as date) as birthdate,
GREATEST(coalesce(CAST(cust.last_update AS datetime), datetime('2000-01-01')), 
           coalesce(CAST(cust_adr.last_update AS datetime), datetime('2000-01-01')), 
		   coalesce(CAST(cust_tel.last_update AS datetime), datetime('2000-01-01')),
           datetime('2000-01-01') ) as last_upd_dt,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(trim(cust_bsc_ad),''), coalesce(trim(cust_supl_ad),''), '', coalesce(trim(cust_cty_nm),''), coalesce(trim(cust_adr_st_prv_cd),''), coalesce(trim(cust_adr_zip_cd),''), '', coalesce(trim(cust_adr_cntry_cd),'')) as address_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(cust_fst_nm),''), coalesce(trim(cust_mid_init_nm),''), coalesce(trim(cust_lst_nm),''), '', coalesce(trim(decode_salutation(cust_sltn_id)),'')) as nm_adw_key,
`{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(phone_concat(cust_tel_area_cd, cust_tel_nr, cust_tel_ext_nr)) as phone_adw_key         
from ( SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY last_update DESC) AS latest_record_check
  FROM `{{ var.value.INGESTION_PROJECT }}.pos.cust` 
  ) cust
left outer join ( SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cust_id, cust_adr_typ_id ORDER BY last_update DESC) AS latest_record_check
  FROM `{{ var.value.INGESTION_PROJECT }}.pos.cust_adr` 
  ) cust_adr
on cust.cust_id=cust_adr.cust_id and 
   cust_adr.latest_record_check=1
left outer join ( SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cust_id, cust_tel_typ_id ORDER BY last_update DESC) AS latest_record_check
  FROM `{{ var.value.INGESTION_PROJECT }}.pos.cust_tel`
  where length(trim(cust_tel_nr))>1  
  ) cust_tel
on cust.cust_id=cust_tel.cust_id
   and cust_tel.latest_record_check=1
where 
cust.latest_record_check=1 and
  GREATEST(coalesce(CAST(cust.last_update AS datetime), datetime('2000-01-01')), 
           coalesce(CAST(cust_adr.last_update AS datetime), datetime('2000-01-01')), 
		   coalesce(CAST(cust_tel.last_update AS datetime), datetime('2000-01-01')),
           datetime('2000-01-01')
	        ) 
			> datetime_sub((select max(effective_start_datetime) from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` where contact_source_system_nm = 'pos'), interval 12 hour)
) pos 
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` name on name.nm_adw_key=pos.nm_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` address on address.address_adw_key=pos.address_adw_key
