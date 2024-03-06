from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_contact_mzp"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22),
    'email': ['airflow@example.com'],
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

######################################################################
# Queries

######################################
# Work queries used to build staging #
######################################
contact_work_source = f"""
--{int_u.INTEGRATION_PROJECT}.
--{iu.INGESTION_PROJECT}.
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_source_work` as
select
membership.membership_ky,
member.member_ky,
438 as iso_cd,
branch.club_cd,
member.membership_id,
member.associate_id,
member.check_digit_nr,
member.customer_id,
coalesce(trim(member.first_name),'') as raw_first_name,
coalesce(trim(member.middle_name),'') as raw_middle_name,
coalesce(trim(member.last_name),'') as raw_last_name,
coalesce(trim(member.name_suffix),'') as raw_suffix_name,
coalesce(trim(member.salutation),'') as raw_title_name,
coalesce(trim(membership.address_line1),'') as raw_address_1_name,
coalesce(trim(membership.address_line2),'') as raw_address_2_name,
cast('' as string) as raw_care_of_name,
coalesce(trim(membership.city),'') as raw_city_name,
coalesce(trim(membership.state),'') as raw_state_code,
coalesce(trim(membership.zip),'') as raw_postal_code,
coalesce(trim(membership.delivery_route),'') as raw_postal_plus_4_code,
coalesce(trim(membership.country),'') as raw_country_name,
coalesce(trim(membership.phone),'') as raw_phone_number,
coalesce(trim(member.email),'') as raw_email_name,
coalesce(trim(member.gender),'') as gender,
safe_cast(substr(member.birth_dt,1,10) as date) as birthdate,
greatest(cast(member.last_upd_dt as datetime), cast(membership.last_upd_dt as datetime)) as last_upd_dt,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(member.first_name))))                                as cleansed_first_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(member.middle_name))))                                as cleansed_middle_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(member.last_name))))                                 as cleansed_last_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(member.name_suffix))))                               as cleansed_suffix_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(member.salutation))))                               as cleansed_title_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    trim(membership.address_line1))                                  as cleansed_address_1_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    trim(membership.address_line2))                                  as cleansed_address_2_name,
cast(null as string)                                                 as cleansed_care_of_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    trim(membership.city))                                           as cleansed_city_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    trim(membership.state))                                          as cleansed_state_code,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    trim(membership.zip))                                            as cleansed_postal_code,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    trim(membership.delivery_route))                                 as cleansed_postal_plus_4_code,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    trim(membership.country))                                        as cleansed_country_name,
coalesce(trim(member.email),'')                                      as cleansed_email_name,
coalesce(trim(membership.phone),'')                                  as cleansed_phone_number
from 
    (select 
      *, 
       row_number() over (partition by member_ky order by last_upd_dt desc) as latest_record_check
      from `{iu.INGESTION_PROJECT}.mzp.member`) member
join (select 
      *, 
       row_number() over (partition by membership_ky order by last_upd_dt desc) as latest_record_check
      from `{iu.INGESTION_PROJECT}.mzp.membership`) membership on member.membership_ky=membership.membership_ky and member.latest_record_check=1 and membership.latest_record_check=1
join (select 
      *, 
      row_number() over (partition by branch_ky order by last_upd_dt desc) as latest_record_check
      from `{iu.INGESTION_PROJECT}.mzp.branch`) branch on membership.branch_ky=branch.branch_ky and branch.latest_record_check=1
where greatest(cast(member.last_upd_dt as datetime), cast(membership.last_upd_dt as datetime)) > (select max(effective_start_datetime) from `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` where contact_source_system_name = 'mzp')

"""
contact_work_target = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_target_work` as 
select 
customer.contact_adw_key as contact_adw_key,
member_key.source_1_key as member_ky,
name.cleansed_first_name  as cleansed_first_name,
name.cleansed_last_name as cleansed_last_name,
name.cleansed_last_name as cleansed_suffix_name,
address.cleansed_address_1_name as cleansed_address_1_name,
address.cleansed_city_name as cleansed_city_name,
address.cleansed_state_code as cleansed_state_code,
address.cleansed_postal_code as cleansed_postal_code,
email.cleansed_email_name as cleansed_email_name
from
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info` customer

left join (select distinct contact_adw_key, source_1_key from `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` where key_type_name='member_key') member_key 
	on customer.contact_adw_key=member_key.contact_adw_key	

left join (select distinct contact_adw_key, name_adw_key from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name`) name_bridge 
  on customer.contact_adw_key=name_bridge.contact_adw_key

left join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name 
  on name_bridge.name_adw_key=name.name_adw_key

left join (select distinct contact_adw_key, address_adw_key from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address`) address_bridge 
  on customer.contact_adw_key=address_bridge.contact_adw_key

left join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address` address 
    on address_bridge.address_adw_key=address.address_adw_key	

left join (select distinct contact_adw_key, email_adw_key from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email`) email_bridge 
  on customer.contact_adw_key=email_bridge.contact_adw_key

left join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email` email 
    on email_bridge.email_adw_key=email.email_adw_key

"""

contact_work_matched = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_matched` as
select
coalesce(member_key_match.contact_adw_key, address_match.contact_adw_key, zip5_match.contact_adw_key, email_match.contact_adw_key) as contact_adw_key,
source.membership_ky,
source.member_ky,
source.iso_cd,
source.club_cd,
source.membership_id,
source.associate_id,
source.check_digit_nr,
source.customer_id,
source.raw_first_name,
source.raw_middle_name,
source.raw_last_name,
source.raw_suffix_name,
source.raw_title_name,
source.raw_address_1_name,
source.raw_address_2_name,
source.raw_care_of_name,
source.raw_city_name,
source.raw_state_code,
source.raw_postal_code,
source.raw_postal_plus_4_code,
source.raw_country_name,
source.raw_phone_number,
source.raw_email_name,
source.gender,
source.birthdate,
source.last_upd_dt,
source.cleansed_first_name,
source.cleansed_middle_name,
source.cleansed_last_name,
source.cleansed_suffix_name,
source.cleansed_title_name,
source.cleansed_address_1_name,
source.cleansed_address_2_name,
source.cleansed_care_of_name,
source.cleansed_city_name,
source.cleansed_state_code,
source.cleansed_postal_code,
source.cleansed_postal_plus_4_code,
source.cleansed_country_name,
source.cleansed_phone_number,
source.cleansed_email_name
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_source_work` source
      
      -- Matching Criteria 1: Member Key --
      left outer join (select max(contact_adw_key) as contact_adw_key
                            , member_ky
                            from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_target_work`
                            group by member_ky
                      ) member_key_match
                        on source.member_ky = member_key_match.member_ky
                        
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
      left outer join (select max(contact_adw_key) as contact_adw_key
                            , cleansed_first_name
                            , cleansed_last_name
                            , cleansed_suffix_name                      
                            , cleansed_address_1_name
                            , cleansed_city_name
                            , cleansed_state_code
                        from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_target_work`
                        group by 
                              cleansed_first_name
                            , cleansed_last_name
                            , cleansed_suffix_name                      
                            , cleansed_address_1_name
                            , cleansed_city_name
                            , cleansed_state_code
                        ) address_match
                           on source.cleansed_first_name      = address_match.cleansed_first_name    
      	                  and source.cleansed_last_name       = address_match.cleansed_last_name     
      	                  and source.cleansed_suffix_name     = address_match.cleansed_suffix_name     
      	                  and source.cleansed_address_1_name  = address_match.cleansed_address_1_name  
      	                  and source.cleansed_city_name       = address_match.cleansed_city_name         
      	                  and source.cleansed_state_code      = address_match.cleansed_state_code        
                          and source.cleansed_first_name      != ''                          
      	                  and source.cleansed_last_name       != ''                                    
      	                  and source.cleansed_address_1_name  != ''                           
      	                  and source.cleansed_city_name       != ''                          
      	                  and source.cleansed_state_code      != ''                          
      
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
      left outer join (select max(contact_adw_key) as contact_adw_key
                             , cleansed_first_name
                             , cleansed_last_name
                             , cleansed_suffix_name                      
                             , cleansed_address_1_name
                             , cleansed_postal_code
                        from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_target_work`
                        group by 
                               cleansed_first_name
                             , cleansed_last_name
                             , cleansed_suffix_name                      
                             , cleansed_address_1_name
                             , cleansed_postal_code
                       ) zip5_match
                          on  source.cleansed_first_name      = zip5_match.cleansed_first_name
                         and source.cleansed_last_name        = zip5_match.cleansed_last_name
                         and source.cleansed_suffix_name      = zip5_match.cleansed_suffix_name                     
      	                 and source.cleansed_address_1_name   = zip5_match.cleansed_address_1_name
      	                 and source.cleansed_postal_code      = zip5_match.cleansed_postal_code
      	                 and source.cleansed_first_name      != ''
      	                 and source.cleansed_last_name       != ''                 
      	                 and source.cleansed_address_1_name  != ''
      	                 and source.cleansed_postal_code     != ''

      -- Matching Criteria 4: First Name, Last Name, Email, Name Suffix --
      left outer join (select max(contact_adw_key) as contact_adw_key
                             , cleansed_first_name
                             , cleansed_last_name
                             , cleansed_suffix_name 
                             , cleansed_email_name
                        from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_target_work`
                        group by 
                               cleansed_first_name
                             , cleansed_last_name
                             , cleansed_suffix_name 
                             , cleansed_email_name
                       ) email_match
                           on  source.cleansed_first_name       = email_match.cleansed_first_name
                           and source.cleansed_last_name        = email_match.cleansed_last_name
                           and source.cleansed_suffix_name      = email_match.cleansed_suffix_name                     
      	                   and source.cleansed_email_name       = email_match.cleansed_email_name
      	                   and source.cleansed_first_name      != ''
      	                   and source.cleansed_last_name       != ''             
      	                   and source.cleansed_email_name      != ''  

"""
contact_work_unmatched_1 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_1` as
with member_guid as
(select
GENERATE_UUID() as contact_adw_key,
member_ky
from (select distinct member_ky from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_matched` where contact_adw_key is null)
) 
select
b.contact_adw_key as contact_adw_key,
original.membership_ky,
original.member_ky,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.customer_id,
original.raw_first_name,
original.raw_middle_name,
original.raw_last_name,
original.raw_suffix_name,
original.raw_title_name,
original.raw_address_1_name,
original.raw_address_2_name,
original.raw_care_of_name,
original.raw_city_name,
original.raw_state_code,
original.raw_postal_code,
original.raw_postal_plus_4_code,
original.raw_country_name,
original.raw_phone_number,
original.raw_email_name,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_name,
original.cleansed_middle_name,
original.cleansed_last_name,
original.cleansed_suffix_name,
original.cleansed_title_name,
original.cleansed_address_1_name,
original.cleansed_address_2_name,
original.cleansed_care_of_name,
original.cleansed_city_name,
original.cleansed_state_code,
original.cleansed_postal_code,
original.cleansed_postal_plus_4_code,
original.cleansed_country_name,
original.cleansed_phone_number,
original.cleansed_email_name        
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_matched` original
left outer join member_guid b on original.member_ky=b.member_ky
where original.contact_adw_key is null

"""

contact_work_unmatched_2 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_2` as
with address_guid as
(
select 
GENERATE_UUID() as contact_adw_key,
                   cleansed_first_name, 
                   cleansed_last_name,
                   cleansed_suffix_name,        
                   cleansed_address_1_name,
                   cleansed_city_name,
                   cleansed_state_code
               from (select distinct 
                            cleansed_first_name
                           ,cleansed_last_name
                           ,cleansed_suffix_name      
                           ,cleansed_address_1_name
                           ,cleansed_city_name
                           ,cleansed_state_code
                      from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_1`
                  where cleansed_first_name     != ''                          
           	        and cleansed_last_name      != ''       
           	        and cleansed_suffix_name    != ''                                                
           	        and cleansed_address_1_name != ''                           
           	        and cleansed_city_name      != ''                          
           	        and cleansed_state_code     != ''                          
                   )
           ),
matched_guid as
           (
                 select distinct
                        a.contact_adw_key as orig_contact_adw_key,
                        min(b.contact_adw_key) as new_contact_adw_key
                   from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_1` a
                        join address_guid b 
                            on 
                                a.cleansed_first_name     = b.cleansed_first_name
                 	         and a.cleansed_last_name      = b.cleansed_last_name
                 	         and a.cleansed_suffix_name    = b.cleansed_suffix_name                     
                 	         and a.cleansed_address_1_name = b.cleansed_address_1_name
                 	         and a.cleansed_city_name      = b.cleansed_city_name
                 	         and a.cleansed_state_code     = b.cleansed_state_code
                 group by 
                            a.contact_adw_key
           )
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.membership_ky,
original.member_ky,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.customer_id,
original.raw_first_name,
original.raw_middle_name,
original.raw_last_name,
original.raw_suffix_name,
original.raw_title_name,
original.raw_address_1_name,
original.raw_address_2_name,
original.raw_care_of_name,
original.raw_city_name,
original.raw_state_code,
original.raw_postal_code,
original.raw_postal_plus_4_code,
original.raw_country_name,
original.raw_phone_number,
original.raw_email_name,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_name,
original.cleansed_middle_name,
original.cleansed_last_name,
original.cleansed_suffix_name,
original.cleansed_title_name,
original.cleansed_address_1_name,
original.cleansed_address_2_name,
original.cleansed_care_of_name,
original.cleansed_city_name,
original.cleansed_state_code,
original.cleansed_postal_code,
original.cleansed_postal_plus_4_code,
original.cleansed_country_name,
original.cleansed_phone_number,
original.cleansed_email_name   
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_1` original
left outer join matched_guid matched on original.contact_adw_key=matched.orig_contact_adw_key


"""
contact_work_unmatched_3 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_3` as
with address_guid as
(
select 
GENERATE_UUID() as contact_adw_key,
                   cleansed_first_name, 
                   cleansed_last_name,
                   cleansed_suffix_name,        
                   cleansed_address_1_name,
                   cleansed_postal_code
               from (select distinct 
                            cleansed_first_name
                           ,cleansed_last_name
                           ,cleansed_suffix_name      
                           ,cleansed_address_1_name
                           ,cleansed_postal_code
                      from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_2`
                  where cleansed_first_name     != ''                          
           	        and cleansed_last_name      != ''       
           	        and cleansed_suffix_name    != ''                                                
           	        and cleansed_address_1_name != ''                                                     
           	        and cleansed_postal_code    != ''
                   )
           ),
matched_guid as
           (
                 select distinct
                        a.contact_adw_key as orig_contact_adw_key,
                        min(b.contact_adw_key) as new_contact_adw_key
                   from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_2` a
                        join address_guid b 
                            on 
                                a.cleansed_first_name     = b.cleansed_first_name
                 	         and a.cleansed_last_name      = b.cleansed_last_name
                 	         and a.cleansed_suffix_name    = b.cleansed_suffix_name                     
                 	         and a.cleansed_address_1_name = b.cleansed_address_1_name
                 	         and a.cleansed_postal_code    = b.cleansed_postal_code
                 group by 
                            a.contact_adw_key
           )
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.membership_ky,
original.member_ky,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.customer_id,
original.raw_first_name,
original.raw_middle_name,
original.raw_last_name,
original.raw_suffix_name,
original.raw_title_name,
original.raw_address_1_name,
original.raw_address_2_name,
original.raw_care_of_name,
original.raw_city_name,
original.raw_state_code,
original.raw_postal_code,
original.raw_postal_plus_4_code,
original.raw_country_name,
original.raw_phone_number,
original.raw_email_name,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_name,
original.cleansed_middle_name,
original.cleansed_last_name,
original.cleansed_suffix_name,
original.cleansed_title_name,
original.cleansed_address_1_name,
original.cleansed_address_2_name,
original.cleansed_care_of_name,
original.cleansed_city_name,
original.cleansed_state_code,
original.cleansed_postal_code,
original.cleansed_postal_plus_4_code,
original.cleansed_country_name,
original.cleansed_phone_number,
original.cleansed_email_name    
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_2` original
left outer join matched_guid matched on original.contact_adw_key=matched.orig_contact_adw_key

"""
contact_work_unmatched_4 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_4` as

with email_guid as
(
    select 
        GENERATE_UUID() as contact_adw_key ,
        cleansed_first_name, 
        cleansed_last_name,
        cleansed_email_name
        from (select distinct 
                     cleansed_first_name
                    ,cleansed_last_name
                    ,cleansed_suffix_name      
                    ,cleansed_email_name
               from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_3`
             where cleansed_first_name     != ''                          
    	        and cleansed_last_name     != ''       
    	        and cleansed_suffix_name   != ''         
    	        and cleansed_email_name    != ''                         
           )
),

matched_guid as
(
     select distinct
         a.contact_adw_key  as orig_contact_adw_key ,
         min(b.contact_adw_key) as new_contact_adw_key 
         from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_3` a
     join email_guid b 
       on a.cleansed_first_name = b.cleansed_first_name
      and a.cleansed_last_name  = b.cleansed_last_name
      and a.cleansed_email_name = b.cleansed_email_name
     group by a.contact_adw_key 
)

select 
coalesce(matched.new_contact_adw_key ,contact_adw_key ) as contact_adw_key ,
original.membership_ky,
original.member_ky,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.customer_id,
original.raw_first_name,
original.raw_middle_name,
original.raw_last_name,
original.raw_suffix_name,
original.raw_title_name,
original.raw_address_1_name,
original.raw_address_2_name,
original.raw_care_of_name,
original.raw_city_name,
original.raw_state_code,
original.raw_postal_code,
original.raw_postal_plus_4_code,
original.raw_country_name,
original.raw_phone_number,
original.raw_email_name,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_name,
original.cleansed_middle_name,
original.cleansed_last_name,
original.cleansed_suffix_name,
original.cleansed_title_name,
original.cleansed_address_1_name,
original.cleansed_address_2_name,
original.cleansed_care_of_name,
original.cleansed_city_name,
original.cleansed_state_code,
original.cleansed_postal_code,
original.cleansed_postal_plus_4_code,
original.cleansed_country_name,
original.cleansed_phone_number,
original.cleansed_email_name
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_3` original

left outer join matched_guid matched on original.contact_adw_key = matched.orig_contact_adw_key 

"""
contact_work_final_staging = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage` as 
select 
contact_adw_key ,
membership_ky,
member_ky,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
customer_id,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_name`(raw_first_name, raw_middle_name, raw_last_name, raw_suffix_name, raw_title_name) as name_adw_key,
raw_first_name,
raw_middle_name,
raw_last_name,
raw_suffix_name,
raw_title_name,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_address`(raw_address_1_name, raw_address_2_name, raw_care_of_name, raw_city_name, raw_state_code, raw_postal_code, raw_postal_plus_4_code, raw_country_name) as address_adw_key,
raw_address_1_name,
raw_address_2_name,
raw_care_of_name,
raw_city_name,
raw_state_code,
raw_postal_code,
raw_postal_plus_4_code,
raw_country_name,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_phone`(raw_phone_number) as phone_adw_key,
raw_phone_number,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_email`(raw_email_name) as email_adw_key,
raw_email_name,
gender,
birthdate,
last_upd_dt,
cleansed_first_name,
cleansed_middle_name,
cleansed_last_name,
cleansed_suffix_name,
cleansed_title_name,
cleansed_address_1_name,
cleansed_address_2_name,
cleansed_care_of_name,
cleansed_city_name,
cleansed_state_code,
cleansed_postal_code,
cleansed_postal_plus_4_code,
cleansed_country_name,
cleansed_phone_number,
cleansed_email_name
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_matched`
where contact_adw_key is not null

union all 

select 
contact_adw_key ,
membership_ky,
member_ky,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
customer_id,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_name`(raw_first_name, raw_middle_name, raw_last_name, raw_suffix_name, raw_title_name) as name_adw_key,
raw_first_name,
raw_middle_name,
raw_last_name,
raw_suffix_name,
raw_title_name,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_address`(raw_address_1_name, raw_address_2_name, raw_care_of_name, raw_city_name, raw_state_code, raw_postal_code, raw_postal_plus_4_code, raw_country_name) as address_adw_key,
raw_address_1_name,
raw_address_2_name,
raw_care_of_name,
raw_city_name,
raw_state_code,
raw_postal_code,
raw_postal_plus_4_code,
raw_country_name,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_phone`(raw_phone_number) as phone_adw_key,
raw_phone_number,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_email`(raw_email_name) as email_adw_key,
raw_email_name,
gender,
birthdate,
last_upd_dt,
cleansed_first_name,
cleansed_middle_name,
cleansed_last_name,
cleansed_suffix_name,
cleansed_title_name,
cleansed_address_1_name,
cleansed_address_2_name,
cleansed_care_of_name,
cleansed_city_name,
cleansed_state_code,
cleansed_postal_code,
cleansed_postal_plus_4_code,
cleansed_country_name,
cleansed_phone_number,
cleansed_email_name

from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_unmatched_work_4`

"""

#################################
# Insert into static dimensions #
#################################
# NAME
query_name_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name`
(name_adw_key,
 raw_first_name,
 raw_middle_name,
 raw_last_name,
 raw_suffix_name,
 raw_title_name,
 cleansed_first_name,
 cleansed_middle_name,
 cleansed_last_name,
 cleansed_suffix_name,
 cleansed_title_name,
 name_validation_status_code,
 name_validation_type,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct
name_adw_key ,
raw_first_name,
raw_middle_name,
raw_last_name,
raw_suffix_name,
raw_title_name,
cleansed_first_name,
cleansed_middle_name,
cleansed_last_name,
cleansed_suffix_name,
cleansed_title_name,
'unverified' as  name_validation_status_code,
'basic cleansing' as name_validation_type,
current_datetime,
1 as batch,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
where name_adw_key not in (select distinct name_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name`)

"""
query_name_bridge_stage = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_name_stage` as
with combined_cust_name as
(
select contact_adw_key,
name_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
union distinct
select
target.contact_adw_key,
target.name_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name` target
where contact_source_system_name='mzp'
 and name_type_code like 'common'
),
cust_name as
(select
contact_adw_key,
name_adw_key,
cast(last_upd_dt as datetime) as effective_start_datetime,
lag(name_adw_key) over (partition by contact_adw_key order by cast(last_upd_dt as datetime)) as prev_name,
coalesce(lead(cast(last_upd_dt as datetime)) over (partition by contact_adw_key order by cast(last_upd_dt as datetime)), datetime('9999-12-31'
) )as next_record_date
from combined_cust_name
),
set_grouping_column as
(select
contact_adw_key,
name_adw_key,
case when prev_name is null or prev_name<>name_adw_key then 1 else 0 end as new_name_tag,
effective_start_datetime,
next_record_date
from cust_name
), set_groups as
(select
contact_adw_key,
name_adw_key,
sum(new_name_tag) over (partition by contact_adw_key order by effective_start_datetime) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
contact_adw_key,
name_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
contact_adw_key,
name_adw_key,
grouping_column
),
update_key_type_name as
(
select
contact_adw_key,
name_adw_key,
effective_start_datetime,
case when effective_end_datetime=datetime('9999-12-31') then effective_end_datetime else datetime_sub(effective_end_datetime, interval 1 second) end as effective_end_datetime,
row_number() over(partition by contact_adw_key,  effective_start_datetime order by effective_end_datetime desc,name_adw_key ) as rn
from deduped
)
select
contact_adw_key,
name_adw_key,
'mzp' as source_system,
concat('common', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as name_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
1 as batch_insert,
current_datetime update_datetime,
1 as batch_update
from update_key_type_name

"""
query_name_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_name_stage` source
on target.contact_adw_key=source.contact_adw_key and 
   target.contact_source_system_name=source.source_system and
   target.name_type_code=source.name_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as datetime) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
contact_adw_key,	
name_adw_key,	
contact_source_system_name,
name_type_code,	
effective_start_datetime,	
effective_end_datetime,	
integrate_insert_datetime,	
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.contact_adw_key,
source.name_adw_key,
source.source_system,
source.name_type,
source.effective_start_datetime,
source.effective_end_datetime,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source and target.contact_source_system_name='mzp' then delete


"""

# ADDRESS
query_address_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address`
(address_adw_key,
 raw_address_1_name,
 raw_address_2_name,
 raw_care_of_name,
 raw_city_name,
 raw_state_code,
 raw_postal_code,
 raw_postal_plus_4_code,
 raw_country_name,
 cleansed_address_1_name,
 cleansed_address_2_name,
 cleansed_care_of_name,
 cleansed_city_name,
 cleansed_state_code,
 cleansed_state_name,
 cleansed_postal_code,
 cleansed_postal_plus_4_code,
 cleansed_country_name,
 latitude_number,
 longitude_number,
 county_name,
 county_fips,
 time_zone_name,
 address_type_code,
 address_validation_status_code,
 address_validation_type,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct 
address_adw_key,
raw_address_1_name,
raw_address_2_name,
raw_care_of_name,
raw_city_name,
raw_state_code,
raw_postal_code,
raw_postal_plus_4_code,
raw_country_name,
cleansed_address_1_name,
cleansed_address_2_name,
cleansed_care_of_name,
cleansed_city_name,
cleansed_state_code,
cast(null as string) cleansed_state_name,
cleansed_postal_code,
cleansed_postal_plus_4_code,
cleansed_country_name,
cast(null as numeric) latitude_number,
cast(null as numeric) longitude_number,
cast(null as string) county_name,
cast(null as string) county_fips,
cast(null as string) time_zone_name,
cast(null as string) address_type_code,
'unverified' address_validation_status_code,
'basic cleansing' address_validation_type,
current_datetime,
1 as batch,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
where address_adw_key not in (select distinct address_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address`)

"""
query_address_bridge_stage = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_address_stage` as
with combined_cust_address as
(
select contact_adw_key,
address_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
union distinct
select
target.contact_adw_key,
target.address_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address` target
where contact_source_system_name='mzp'
 and address_type_code like 'home%'
),
cust_address as
(select
contact_adw_key,
address_adw_key,
cast(last_upd_dt as datetime) as effective_start_datetime,
lag(address_adw_key) over (partition by contact_adw_key order by cast(last_upd_dt as datetime)) as prev_address,
coalesce(lead(cast(last_upd_dt as datetime)) over (partition by contact_adw_key order by cast(last_upd_dt as datetime)), datetime('9999-12-31'
) )as next_record_date
from combined_cust_address
),
set_grouping_column as
(select
contact_adw_key,
address_adw_key,
case when prev_address is null or prev_address<>address_adw_key then 1 else 0 end as new_address_tag,
effective_start_datetime,
next_record_date
from cust_address
), set_groups as
(select
contact_adw_key,
address_adw_key,
sum(new_address_tag) over (partition by contact_adw_key order by effective_start_datetime) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
contact_adw_key,
address_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
contact_adw_key,
address_adw_key,
grouping_column
),

update_key_type_name as
(
select
contact_adw_key,
address_adw_key,
effective_start_datetime,
case when effective_end_datetime=datetime('9999-12-31') then effective_end_datetime else datetime_sub(effective_end_datetime, interval 1 second) end as effective_end_datetime,
row_number() over(partition by contact_adw_key,  effective_start_datetime order by effective_end_datetime desc,address_adw_key ) as rn
from deduped
)

select
contact_adw_key,
address_adw_key,
'mzp' as source_system,
concat('home', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as address_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
1 as batch_insert,
current_datetime update_datetime,
1 as batch_update
from update_key_type_name

"""
query_address_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_address_stage` source
on target.contact_adw_key=source.contact_adw_key and 
   target.contact_source_system_name=source.source_system and
   target.address_type_code=source.address_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as datetime) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update,
target.active_indicator=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END
when not matched by target then insert
(
contact_adw_key,	
address_adw_key,	
contact_source_system_name,
address_type_code,	
effective_start_datetime,	
effective_end_datetime,	
active_indicator,
integrate_insert_datetime,	
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.contact_adw_key,
source.address_adw_key,
source.source_system,
source.address_type,
source.effective_start_datetime,
source.effective_end_datetime,
CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source and target.contact_source_system_name='mzp' then delete

"""

# PHONE
query_phone_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone`
(phone_adw_key,
 raw_phone_number,
 cleansed_phone_number,
 cleansed_phone_area_code_number,
 cleansed_phone_prefix_number,
 cleansed_phone_suffix_number,
 cleansed_phone_extension_number,
 phone_validation_status_code,
 phone_validation_type,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct
phone_adw_key,
raw_phone_number,
cleansed_phone_number,
cast(null as int64) cleansed_phone_area_code_number,
cast(null as int64) cleansed_phone_prefix_number,
cast(null as int64) cleansed_phone_suffix_number,
cast(null as int64) cleansed_phone_extension_number,
'unverified' phone_validation_status_code,
'basic cleansing' phone_validation_type,
current_datetime,
1 as batch,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
where phone_adw_key not in (select distinct phone_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone`)
and raw_phone_number is not null

"""
query_phone_bridge_stage = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_phone_stage` as
with combined_cust_phone as
(
select contact_adw_key,
phone_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
union distinct
select
target.contact_adw_key,
target.phone_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_phone` target
where contact_source_system_name='mzp'
 and phone_type_code like 'home%'
),
cust_phone as
(select
contact_adw_key,
phone_adw_key,
cast(last_upd_dt as datetime) as effective_start_datetime,
lag(phone_adw_key) over (partition by contact_adw_key order by cast(last_upd_dt as datetime)) as prev_phone,
coalesce(lead(cast(last_upd_dt as datetime)) over (partition by contact_adw_key order by cast(last_upd_dt as datetime)), datetime('9999-12-31'
) )as next_record_date
from combined_cust_phone
),
set_grouping_column as
(select
contact_adw_key,
phone_adw_key,
case when prev_phone is null or prev_phone<>phone_adw_key then 1 else 0 end as new_phone_tag,
effective_start_datetime,
next_record_date
from cust_phone
), set_groups as
(select
contact_adw_key,
phone_adw_key,
sum(new_phone_tag) over (partition by contact_adw_key order by effective_start_datetime) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
contact_adw_key,
phone_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
contact_adw_key,
phone_adw_key,
grouping_column
),
update_key_type_name as
(
select
contact_adw_key,
phone_adw_key,
effective_start_datetime,
case when effective_end_datetime=datetime('9999-12-31') then effective_end_datetime else datetime_sub(effective_end_datetime, interval 1 second) end as effective_end_datetime,
row_number() over(partition by contact_adw_key,  effective_start_datetime order by effective_end_datetime desc,phone_adw_key ) as rn
from deduped
)
select
contact_adw_key,
phone_adw_key,
'mzp' as source_system,
concat('home', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as phone_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
1 as batch_insert,
current_datetime update_datetime,
1 as batch_update
from update_key_type_name

"""
query_phone_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_phone` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_phone_stage` source
on target.contact_adw_key=source.contact_adw_key and 
   target.contact_source_system_name=source.source_system and
   target.phone_type_code=source.phone_type and
   target.effective_start_datetime=source.effective_start_datetime and
   target.active_indicator=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END
when matched and target.effective_end_datetime=cast('9999-12-31' as datetime) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
contact_adw_key,	
phone_adw_key,	
contact_source_system_name,
phone_type_code,	
effective_start_datetime,	
effective_end_datetime,	
active_indicator,
integrate_insert_datetime,	
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.contact_adw_key,
source.phone_adw_key,
source.source_system,
source.phone_type,
source.effective_start_datetime,
source.effective_end_datetime,
CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source and target.contact_source_system_name='mzp' then delete

"""

# EMAIL
query_email_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email`
(email_adw_key,
 raw_email_name,
 cleansed_email_name,
 email_top_level_domain_name,
 email_domain_name,
 email_validation_status_code,
 email_validation_type,
 integrate_insert_datetime,
 integrate_insert_batch_number,
 integrate_update_datetime,
 integrate_update_batch_number
)
select
distinct
email_adw_key,
raw_email_name,
cleansed_email_name,
cast(null as string) email_top_level_domain_name,
cast(null as string) email_domain_name,
'unverified' email_validation_status_code,
'basic cleansing' email_validation_type,
current_datetime,
1 as batch,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
where email_adw_key not in (select distinct email_adw_key  from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email`)
and raw_email_name is not null and raw_email_name != ''

"""
query_email_bridge_stage = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_email_stage` as
with combined_cust_email as
(
select contact_adw_key,
email_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
union distinct
select
target.contact_adw_key,
target.email_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email` target
where contact_source_system_name='mzp'
 and email_type_code like 'personal%'
),
cust_email as
(select
contact_adw_key,
email_adw_key,
cast(last_upd_dt as datetime) as effective_start_datetime,
lag(email_adw_key) over (partition by contact_adw_key order by cast(last_upd_dt as datetime), email_adw_key) as prev_email,
coalesce(lead(cast(last_upd_dt as datetime)) over (partition by contact_adw_key order by cast(last_upd_dt as datetime), email_adw_key), datetime('9999-12-31' ) )as next_record_date
from combined_cust_email
),
set_grouping_column as
(select
contact_adw_key,
email_adw_key,
case when prev_email is null or prev_email<>email_adw_key then 1 else 0 end as new_email_tag,
effective_start_datetime,
next_record_date
from cust_email
), set_groups as
(select
contact_adw_key,
email_adw_key,
sum(new_email_tag) over (partition by contact_adw_key order by effective_start_datetime, email_adw_key) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
contact_adw_key,
email_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
contact_adw_key,
email_adw_key,
grouping_column
), update_key_type_name as
(
select
contact_adw_key,
email_adw_key,
effective_start_datetime,
case when effective_end_datetime=datetime('9999-12-31') then effective_end_datetime else datetime_sub(effective_end_datetime, interval 1 second) end as effective_end_datetime,
row_number() over(partition by contact_adw_key,  effective_start_datetime order by effective_end_datetime desc,email_adw_key ) as rn
from deduped
)
select
contact_adw_key,
email_adw_key,
'mzp' as source_system,
concat('personal', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as email_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
1 as batch_insert,
current_datetime update_datetime,
1 as batch_update
from update_key_type_name
where email_adw_key != TO_BASE64(MD5(''))

"""
query_email_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_email_stage` source
on target.contact_adw_key=source.contact_adw_key and 
   target.contact_source_system_name=source.source_system and
   target.email_type_code=source.email_type and
   target.effective_start_datetime=source.effective_start_datetime and
   target.active_indicator=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END
when matched and target.effective_end_datetime=cast('9999-12-31' as datetime) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
contact_adw_key,	
email_adw_key,	
contact_source_system_name,
email_type_code,	
effective_start_datetime,	
effective_end_datetime,	
active_indicator,
integrate_insert_datetime,	
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.contact_adw_key,
source.email_adw_key,
source.source_system,
source.email_type,
source.effective_start_datetime,
source.effective_end_datetime,
CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source and target.contact_source_system_name='mzp' then delete

"""

# KEYS
query_source_key_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_source_key_stage` AS
WITH
  normalized_source_key AS (
  SELECT
    contact_adw_key,
    'membership_key' AS key_type_name,
    CAST(last_upd_dt AS datetime) last_upd_dt,
    membership_ky AS source_1_key,
    CAST(NULL AS string) AS source_2_key,
    CAST(NULL AS string) AS source_3_key,
    CAST(NULL AS string) AS source_4_key,
    CAST(NULL AS string) AS source_5_key,
    membership_ky AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
  UNION DISTINCT
  SELECT
    contact_adw_key,
    'member_key' AS key_type_name,
    CAST(last_upd_dt AS datetime) last_upd_dt,
    member_ky AS source_1_key,
    CAST(NULL AS string) AS source_2_key,
    CAST(NULL AS string) AS source_3_key,
    CAST(NULL AS string) AS source_4_key,
    CAST(NULL AS string) AS source_5_key,
    member_ky AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
  UNION DISTINCT
  SELECT
    contact_adw_key,
    'member_id' AS key_type_name,
    CAST(last_upd_dt AS datetime) last_upd_dt,
    CAST(iso_cd AS string) AS source_1_key,
    club_cd AS source_2_key,
    membership_id AS source_3_key,
    associate_id AS source_4_key,
    check_digit_nr AS source_5_key,
    CONCAT(ifnull(CAST(iso_cd AS string),
        ''),ifnull(club_cd,
        ''),ifnull(membership_id,
        ''),ifnull(associate_id,
        ''),ifnull(check_digit_nr,
        '')) AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
  UNION DISTINCT
  SELECT
    contact_adw_key,
    'customer_id' AS key_type_name,
    CAST(last_upd_dt AS datetime) last_upd_dt,
    customer_id AS source_1_key,
    CAST(NULL AS string) AS source_2_key,
    CAST(NULL AS string) AS source_3_key,
    CAST(NULL AS string) AS source_4_key,
    CAST(NULL AS string) AS source_5_key,
    customer_id AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage` ),
  all_keys AS (
  SELECT
    contact_adw_key,
    case 
        when key_type_name like 'member_id%' then 'member_id'
        when key_type_name like 'membership_key%' then 'membership_key'
        when key_type_name like 'member_key%' then 'member_key'
    else 'customer_id'
  END
    AS key_type_name,
    effective_start_datetime AS last_upd_dt,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    CASE
      WHEN key_type_name like 'member_id%' THEN CONCAT(ifnull(CAST(source_1_key AS string), ''), ifnull(source_2_key, ''), ifnull(source_3_key, ''), ifnull(source_4_key, ''), ifnull(source_5_key, ''))
    ELSE
    source_1_key
  END
    AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key`
  WHERE
    contact_source_system_name = 'mzp'
  UNION DISTINCT
  SELECT
    *
  FROM
    normalized_source_key ),
  cust_keys AS (
  SELECT
    contact_adw_key,
    key_type_name,
    last_upd_dt AS effective_start_datetime,
    LAG(dedupe_check) OVER (PARTITION BY contact_adw_key, key_type_name ORDER BY last_upd_dt, dedupe_check) AS prev_dedupe_check,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY contact_adw_key, key_type_name ORDER BY last_upd_dt, dedupe_check),
      DATETIME('9999-12-31' ) )AS next_record_date,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    dedupe_check
  FROM
    all_keys ),
  set_grouping_column AS (
  SELECT
    contact_adw_key,
    key_type_name,
    CASE
      WHEN dedupe_check IS NULL OR prev_dedupe_check<>dedupe_check THEN 1
    ELSE
    0
  END
    AS new_key_tag,
    effective_start_datetime,
    next_record_date,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    dedupe_check
  FROM
    cust_keys ),
  set_groups AS (
  SELECT
    contact_adw_key,
    key_type_name,
    SUM(new_key_tag) OVER (PARTITION BY contact_adw_key, key_type_name ORDER BY effective_start_datetime, dedupe_check) AS grouping_column,
    effective_start_datetime,
    next_record_date,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key
  FROM
    set_grouping_column ),
  deduped AS (
  SELECT
    contact_adw_key,
    key_type_name,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_date) AS effective_end_datetime,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key
  FROM
    set_groups
  GROUP BY
    contact_adw_key,
    key_type_name,
    grouping_column,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key ),
  update_key_type_name AS (
  SELECT
    contact_adw_key,
	'mzp' AS contact_source_system_name,
    key_type_name,
    effective_start_datetime,
    case when effective_end_datetime=datetime('9999-12-31') then effective_end_datetime else datetime_sub(effective_end_datetime, interval 1 second) end as effective_end_datetime,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    ROW_NUMBER() OVER(PARTITION BY contact_adw_key, key_type_name, effective_start_datetime ORDER BY effective_end_datetime DESC, source_1_key, source_2_key, source_3_key, source_4_key, source_5_key) AS rn
  FROM
    deduped )
SELECT
  contact_adw_key,
  contact_source_system_name,
  CASE
    WHEN rn=1 THEN key_type_name
  ELSE
  CONCAT(key_type_name,'-',CAST(rn AS string))
END
  AS key_type_name,
  effective_start_datetime,
  effective_end_datetime,
  source_1_key,
  source_2_key,
  source_3_key,
  source_4_key,
  source_5_key,
  current_datetime insert_datetime,
  1 AS batch_insert,
  current_datetime update_datetime,
  1 AS batch_update,
  TO_BASE64(MD5(CONCAT(ifnull(contact_source_system_name,
          ''),'|',ifnull(CASE WHEN rn=1 THEN key_type_name ELSE CONCAT(key_type_name,'-',CAST(rn AS string)) END,
          ''),'|',ifnull(source_1_key,
          ''),'|',ifnull(source_2_key,
          ''),'|',ifnull(source_3_key,
          ''),'|',ifnull(source_4_key,
          ''),'|',ifnull(source_5_key,			  			  			  
          '') ))) AS adw_row_hash
FROM
  update_key_type_name  as prep_stage
  where not exists
  (select 1 from `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` as target
	  where target.contact_adw_key=prep_stage.contact_adw_key and 
            target.contact_source_system_name=prep_stage.contact_source_system_name and
            target.key_type_name=prep_stage.key_type_name and
            target.effective_start_datetime=prep_stage.effective_start_datetime and
			target.adw_row_hash <> (TO_BASE64(MD5(CONCAT(ifnull(prep_stage.contact_source_system_name,
                                    ''),'|',ifnull(CASE WHEN prep_stage.rn=1 THEN prep_stage.key_type_name 
										                ELSE CONCAT(prep_stage.key_type_name,'-',CAST(prep_stage.rn AS string)) 
													END,
                                    ''),'|',ifnull(prep_stage.source_1_key,
                                    ''),'|',ifnull(prep_stage.source_2_key,
                                    ''),'|',ifnull(prep_stage.source_3_key,
                                    ''),'|',ifnull(prep_stage.source_4_key,
                                    ''),'|',ifnull(prep_stage.source_5_key,			  			  			  
                                    '') ))))
		)

"""
query_source_key_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_source_key_stage` source
on target.contact_adw_key=source.contact_adw_key and 
   target.contact_source_system_name=source.contact_source_system_name and
   target.key_type_name=source.key_type_name and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as datetime) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update,
target.active_indicator=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END
when not matched by target then insert
(
contact_adw_key,
contact_source_system_name,
key_type_name,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
source_1_key,
source_2_key,
source_3_key,
source_4_key,
source_5_key,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.contact_adw_key,
source.contact_source_system_name,
source.key_type_name,
source.effective_start_datetime,
source.effective_end_datetime,
CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END,
adw_row_hash,
source.source_1_key,
source.source_2_key,
source.source_3_key,
source.source_4_key,
source.source_5_key,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source and target.contact_source_system_name='mzp' then delete

"""

# CUSTOMER
query_contact_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info`
(contact_adw_key ,    
contact_gender_code,    
contact_gender_name,    
contact_birth_date,
adw_row_hash,
integrate_insert_datetime,    
integrate_insert_batch_number,    
integrate_update_datetime,    
integrate_update_batch_number
)
select
contact_adw_key ,
max(gender) as contact_gender_code,
cast(null as string) as contact_gender_name,
max(birthdate) as birth_date,
'-1' as adw_row_hash,--default initial insert
current_datetime,
1 as batch_insert,
current_datetime,
1 as batch_update
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_contact_stage`
where contact_adw_key not in (select distinct contact_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info`)
group by contact_adw_key 

 """
query_contact_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info` target
using
(
select 
customer.contact_adw_key,
name.raw_first_name,
name.raw_middle_name,
name.raw_last_name,
name.raw_suffix_name,
name.raw_title_name,
address.raw_address_1_name,
address.raw_city_name,
address.raw_state_code,
address.raw_postal_code,
address.raw_postal_plus_4_code,
address.raw_country_name,
phone.raw_phone_number,
phone.cleansed_phone_number,
phone.cleansed_phone_extension_number,
email.raw_email_name,
customer.contact_birth_date,
(TO_BASE64(MD5(CONCAT(ifnull(name.raw_first_name,''),'|'
                     ,ifnull(name.raw_middle_name,''),'|'
                     ,ifnull(name.raw_last_name ,''),'|'				
                     ,ifnull(name.raw_suffix_name,''),'|'
                     ,ifnull(name.raw_title_name,''),'|'									 
                     ,ifnull(address.raw_address_1_name,''),'|'
                     ,ifnull(address.raw_address_2_name,''),'|'									 
                     ,ifnull(address.raw_care_of_name,''),'|'
                     ,ifnull(address.raw_city_name,''),'|'									 
                     ,ifnull(address.raw_state_code,''),'|'								 
                     ,ifnull(address.raw_postal_code,''),'|'
                     ,ifnull(address.raw_postal_plus_4_code,''),'|'									 
                     ,ifnull(address.raw_country_name,''),'|'
                     ,ifnull(phone.raw_phone_number,''),'|'									 
                     ,ifnull(phone.cleansed_phone_number,''),'|'
                     ,ifnull(cast(phone.cleansed_phone_extension_number as string),''),'|'									 
                     ,ifnull(email.raw_email_name,''),'|'
                     ,'','|'
                     ,'','|'
                     ,ifnull(safe_cast(customer.contact_birth_date as string),'') 
                      )))) as adw_row_hash
from
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info` customer
join 
(select
contact_adw_key,
contact_source_system_name,
case when contact_source_system_name='mzp' then 'membership'
     when contact_source_system_name='d3' then 'caller'
	 when contact_source_system_name='epic' then 'client' end as type_code,
row_number() over(partition by contact_adw_key order by case when contact_source_system_name='mzp' then 1 
                                                             when contact_source_system_name='epic' then 2 
															 when contact_source_system_name='d3' then 3 end asc, 
														effective_end_datetime desc, 
														effective_start_datetime desc) as rn
from `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key`
) primacy on customer.contact_adw_key=primacy.contact_adw_key	and primacy.rn=1

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name` name_bridge 
    on customer.contact_adw_key=name_bridge.contact_adw_key and
	name_bridge.contact_source_system_name=primacy.contact_source_system_name and
	name_bridge.name_type_code=primacy.type_code and
	name_bridge.effective_end_datetime=datetime('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name 
    on name_bridge.name_adw_key=name.name_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address` address_bridge 
    on customer.contact_adw_key=address_bridge.contact_adw_key and
	address_bridge.contact_source_system_name=primacy.contact_source_system_name and
	address_bridge.address_type_code=primacy.type_code and
	address_bridge.effective_end_datetime=datetime('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address` address 
    on address_bridge.address_adw_key=address.address_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email` email_bridge 
    on customer.contact_adw_key=email_bridge.contact_adw_key and
	email_bridge.contact_source_system_name=primacy.contact_source_system_name and
	email_bridge.email_type_code=primacy.type_code and
	email_bridge.effective_end_datetime=datetime('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email` email 
    on email_bridge.email_adw_key=email.email_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_phone` phone_bridge 
    on customer.contact_adw_key=phone_bridge.contact_adw_key and
	phone_bridge.contact_source_system_name=primacy.contact_source_system_name and
	phone_bridge.phone_type_code=primacy.type_code and
	phone_bridge.effective_end_datetime=datetime('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone` phone 
    on phone_bridge.phone_adw_key=phone.phone_adw_key

) source
on source.contact_adw_key=target.contact_adw_key
when matched and source.adw_row_hash<>target.adw_row_hash then update
set 
target.contact_first_name=source.raw_first_name,
target.contact_middle_name=source.raw_middle_name,
target.contact_last_name=source.raw_last_name,
target.contact_suffix_name=source.raw_suffix_name,
target.contact_title_name=source.raw_title_name,
target.contact_address_1_name=source.raw_address_1_name,
target.contact_city_name=source.raw_city_name,
target.contact_state_code=source.raw_state_code,
target.contact_postal_code=source.raw_postal_code,
target.contact_postal_plus_4_code=source.raw_postal_plus_4_code,
target.contact_country_name=source.raw_country_name,
target.contact_email_name=source.raw_email_name,
target.contact_phone_number=source.raw_phone_number,
target.contact_phone_formatted_number=source.cleansed_phone_number,
target.contact_phone_extension_number=source.cleansed_phone_extension_number,
target.integrate_update_datetime=current_datetime,
target.integrate_update_batch_number=1

"""

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    # Matching Logic
    task_contact_work_source = int_u.run_query("Prepare_adw_work_mzp_contact_source_work", contact_work_source)
    task_contact_work_target = int_u.run_query("Prepare_adw_work_mzp_contact_target_work", contact_work_target)
    task_contact_work_matched = int_u.run_query("Prepare_cadw_work_mzp_contact_matched", contact_work_matched)
    task_contact_work_unmatched_1 = int_u.run_query("Prepare_adw_work_mzp_contact_unmatched_work_1",
                                                    contact_work_unmatched_1)
    task_contact_work_unmatched_2 = int_u.run_query("Prepare_adw_work_mzp_contact_unmatched_work_2",
                                                    contact_work_unmatched_2)
    task_contact_work_unmatched_3 = int_u.run_query("Prepare_adw_work_mzp_contact_unmatched_work_3",
                                                    contact_work_unmatched_3)
    task_contact_work_unmatched_4 = int_u.run_query("Prepare_adw_work_mzp_contact_unmatched_work_4",
                                                    contact_work_unmatched_4)

    task_contact_work_final_staging = int_u.run_query("Prepare_adw_work_mzp_contact_stage", contact_work_final_staging)

    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_dim_name', query_name_insert)
    task_name_bridge_stage = int_u.run_query('Prepare_adw_work_mzp_contact_name_stage', query_name_bridge_stage, 2)
    task_name_merge = int_u.run_query('merge_xref_contact_name', query_name_merge)

    ## address
    task_address_insert = int_u.run_query('insert_dim_address', query_address_insert)
    task_address_bridge_stage = int_u.run_query('Prepare_adw_work_mzp_contact_address_stage',
                                                query_address_bridge_stage, 2)
    task_address_merge = int_u.run_query('merge_xref_contact_address', query_address_merge)

    ## phone
    task_phone_insert = int_u.run_query('insert_dim_phone', query_phone_insert)
    task_phone_bridge_stage = int_u.run_query('Prepare_adw_work.mzp_contact_phone_stage', query_phone_bridge_stage, 2)
    task_phone_merge = int_u.run_query('merge_xref_contact_phone', query_phone_merge)

    ## email
    task_email_insert = int_u.run_query('insert_dim_email', query_email_insert)
    task_email_bridge_stage = int_u.run_query('Prepare_adw_work_mzp_contact_email_stage', query_email_bridge_stage, 2)
    task_email_merge = int_u.run_query('merge_xref_contact_email', query_email_merge)

    ## keys
    task_keys_stage = int_u.run_query('Prepare_adw_work_mzp_contact_source_key_stage', query_source_key_stage)
    task_keys_merge = int_u.run_query('merge_dim_contact_source_key', query_source_key_merge)

    # contact
    task_contact_insert = int_u.run_query('insert_dim_contact_info', query_contact_insert)
    task_contact_merge = int_u.run_query('merge_dim_contact_info', query_contact_merge)

    ######################################################################
    # DEPENDENCIES

    task_contact_work_source >> task_contact_work_matched
    task_contact_work_target >> task_contact_work_matched

    task_contact_work_matched >> task_contact_work_unmatched_1 >> task_contact_work_unmatched_2 >> task_contact_work_unmatched_3 >> task_contact_work_unmatched_4 >> task_contact_work_final_staging

    task_contact_work_final_staging >> task_name_insert >> task_name_bridge_stage
    task_name_bridge_stage >> task_name_merge >> task_contact_insert

    task_contact_work_final_staging >> task_address_insert >> task_address_bridge_stage
    task_address_bridge_stage >> task_address_merge >> task_contact_insert
    task_contact_work_final_staging >> task_phone_insert >> task_phone_bridge_stage
    task_phone_bridge_stage >> task_phone_merge >> task_contact_insert

    task_contact_work_final_staging >> task_email_insert >> task_email_bridge_stage

    task_email_bridge_stage >> task_email_merge >> task_contact_insert

    task_contact_work_final_staging >> task_keys_stage >> task_keys_merge >> task_contact_insert

    task_contact_insert >> task_contact_merge
