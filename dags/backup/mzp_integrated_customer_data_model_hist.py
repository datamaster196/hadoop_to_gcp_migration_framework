from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_customer_mzp_hist"

default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'start_date'      : datetime(2019, 8, 22),
    'email'           : ['airflow@example.com'],
    'catchup'         : False,
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=5),
}

######################################################################
# Queries

######################################
# Work queries used to build staging #
######################################
customer_work_source = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_source_work` as
select
membership.membership_ky,
member.member_ky,
438 as iso_cd,
branch.club_cd,
member.membership_id,
member.associate_id,
member.check_digit_nr,
member.customer_id,
trim(member.first_name) as first_name,
trim(member.middle_name) as middle_name,
trim(member.last_name) as last_name,
trim(member.name_suffix) as name_suffix,
trim(member.salutation) as salutation,
trim(membership.address_line1) as address_line1,
trim(membership.address_line2) as address_line2,
trim(membership.city) as city,
trim(membership.state) as state,
trim(membership.zip) as zip,
trim(membership.delivery_route) as zip_4,
trim(membership.country) as country,
trim(membership.phone) as phone,
trim(member.email) as email,
trim(member.gender) as gender,
safe_cast(substr(member.birth_dt,1,10) as date) as birthdate,
coalesce(least(cast(member.last_upd_dt as datetime), cast(membership.last_upd_dt as datetime)), least(cast(member.status_dt as datetime), cast(membership.status_dt as datetime))) as last_upd_dt,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
            trim(member.first_name))))                                as cleansed_first_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
            trim(member.last_name))))                                 as cleansed_last_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
            trim(member.name_suffix))))                               as cleansed_name_suffix,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
    trim(membership.address_line1))                                  as cleansed_address_line1,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
    trim(membership.city))                                           as cleansed_city,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
    trim(membership.state))                                          as cleansed_state,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
trim(membership.zip))                                                as cleansed_zip,
trim(member.email)                                                    as cleansed_email
from 
    (select 
      *, 
       coalesce(lead(cast(last_upd_dt as  datetime)) over (partition by member_ky order by last_upd_dt), datetime('9999-12-31')) as eff_end_dt
      from `{iu.INGESTION_PROJECT}.mzp.member_hist`) member
join (select 
      *, 
       coalesce(lead(cast(last_upd_dt as  datetime)) over (partition by membership_ky order by last_upd_dt), datetime('9999-12-31')) as eff_end_dt
      from `{iu.INGESTION_PROJECT}.mzp.membership_hist`) membership on member.membership_ky=membership.membership_ky and  cast(member.last_upd_dt as datetime)<=membership.eff_end_dt and cast(membership.last_upd_dt as datetime)<=member.eff_end_dt
join (select 
      *, 
      row_number() over (partition by branch_ky order by last_upd_dt desc) as rn
      from `{iu.INGESTION_PROJECT}.mzp.branch_hist`) branch on membership.branch_ky=branch.branch_ky and branch.rn=1
"""
customer_work_target = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_target_work` as 
select 
customer.customer_adw_key as customer_adw_key,
member_key.customer_source_1_key as member_ky,
name.name_first_name first_name,
name.name_middle_name middle_name,
name.name_last_name last_name,
name.name_suffix_name name_suffix,
name.name_title_name salutation,
address.address_1_name address_line1,
address.address_city_name city,
address.address_state_code state,
address.address_postal_code zip,
address.address_postal_plus_4_code,
address.address_country_name country,
email.email_name email,
customer.customer_birth_date birthdate

`{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
     trim(email.email_name))                                          as cleansed_email,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
     `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
         `{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
             trim(name.name_first_name))))                             as cleansed_first_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
     `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
         `{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
              trim(name.name_last_name))))                              as cleansed_last_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
     `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
         `{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
              trim(name.name_suffix_name))))                            as cleansed_name_suffix,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
      trim(address.address_1_name))                                    as cleansed_address_line1,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
      trim(address.address_city_name))                                as cleansed_city,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
      trim(address.address_state_code))                               as cleansed_state,
`{int_u.INTEGRATION_PROJECT}.udf_remove_selected_punctuation`(
      trim(address.address_postal_code))                              as cleansed_zip
from
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_customer` customer

join `{int_u.INTEGRATION_PROJECT}.adw.dim_customer_source_key` member_key 
	on customer.customer_adw_key=member_key.customer_adw_key and 
	member_key.customer_source_system_name='mzp' and 
	member_key.customer_key_type_name='member_key' and
	member_key.effective_end_datetime=date('9999-12-31')	

join `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_name` name_bridge 
  on customer.customer_adw_key=name_bridge.customer_adw_key and
	name_bridge.customer_source_system_name='mzp' and
	name_bridge.name_type_code='membership' and
	name_bridge.effective_end_datetime=date('9999-12-31')

join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name 
  on name_bridge.name_adw_key=name.name_adw_key

join `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_address` address_bridge 
  on customer.customer_adw_key=address_bridge.customer_adw_key and
	address_bridge.customer_source_system_name='mzp' and
	address_bridge.address_type_code='membership' and
	address_bridge.effective_end_datetime=date('9999-12-31')

join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address` address 
    on address_bridge.address_adw_key=address.address_adw_key	

left join `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_email` email_bridge 
  on customer.customer_adw_key=email_bridge.customer_adw_key and
	email_bridge.customer_source_system_name='mzp' and
	email_bridge.email_type_code='membership' and
	email_bridge.effective_end_datetime=date('9999-12-31')

left join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email` email 
    on email_bridge.email_adw_key=email.email_adw_key

"""
customer_work_matched = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_matched` as
select
coalesce(member_key_match.customer_adw_key, address_match.customer_adw_key, email_match.customer_adw_key) as customer_adw_key,
source.membership_ky,
source.member_ky,
source.iso_cd,
source.club_cd,
source.membership_id,
source.associate_id,
source.check_digit_nr,
source.customer_id,
source.first_name,
source.middle_name,
source.last_name,
source.name_suffix,
source.salutation,
source.address_line1,
source.address_line2,
source.city,
source.state,
source.zip,
source.zip_4,
source.country,
source.phone,
source.email,
source.gender,
source.birthdate,
source.last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_source_work` source
      
      -- Matching Criteria 1: Member Key --
      left outer join (select max(customer_adw_key) as customer_adw_key
                            , member_ky
                            from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_target_work`
                            group by member_ky
                      ) member_key_match
                        on source.member_ky = member_key_match.member_ky
                        
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
      left outer join (select max(customer_adw_key) as customer_adw_key
                            , cleansed_first_name
                            , cleansed_last_name
                            , cleansed_name_suffix                      
                            , cleansed_address_line1
                            , cleansed_city
                            , cleansed_state
                        from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_target_work`
                        group by 
                              cleansed_first_name
                            , cleansed_last_name
                            , cleansed_name_suffix                      
                            , cleansed_address_line1
                            , cleansed_city
                            , cleansed_state
                        ) address_match
                           on source.cleansed_first_name      = address_match.cleansed_first_name    
      	                  and source.cleansed_last_name       = address_match.cleansed_last_name     
      	                  and source.cleansed_name_suffix     = address_match.cleansed_name_suffix     
      	                  and source.cleansed_address_line1   = address_match.cleansed_address_line1  
      	                  and source.cleansed_city            = address_match.cleansed_city         
      	                  and source.cleansed_state           = address_match.cleansed_state        
                          and source.cleansed_first_name      != ''                          
      	                  and source.cleansed_last_name       != ''                                    
      	                  and source.cleansed_address_line1   != ''                           
      	                  and source.cleansed_city            != ''                          
      	                  and source.cleansed_state           != ''                          
      
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
      left outer join (select max(customer_adw_key) as customer_adw_key
                             , cleansed_first_name
                             , cleansed_last_name
                             , cleansed_name_suffix                      
                             , cleansed_address_line1
                             , cleansed_zip
                        from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_target_work`
                        group by 
                             , cleansed_first_name
                             , cleansed_last_name
                             , cleansed_name_suffix                      
                             , cleansed_address_line1
                             , cleansed_zip
                       ) zip5_match
                          on  source.cleansed_first_name      = zip5_match.cleansed_first_name
                         and source.cleansed_last_name        = zip5_match.cleansed_last_name
                         and source.cleansed_name_suffix      = zip5_match.cleansed_name_suffix                     
      	                 and source.cleansed_address_line1    = zip5_match.cleansed_address_line1
      	                 and source.cleansed_zip              = zip5_match.cleansed_zip
      	                 and source.cleansed_first_name      != ''
      	                 and source.cleansed_last_name       != ''
                         and source.cleansed_name_suffix     != ''                  
      	                 and source.cleansed_address_line1   != ''
      	                 and source.cleansed_zip             != ''

      -- Matching Criteria 4: First Name, Last Name, Email, Name Suffix --
      left outer join (select max(customer_adw_key) as customer_adw_key
                             , cleansed_first_name
                             , cleansed_last_name
                             , cleansed_name_suffix 
                             , cleansed_email
                        from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_target_work`
                        group by 
                               cleansed_first_name
                             , cleansed_last_name
                             , cleansed_name_suffix 
                             , cleansed_email
                       ) email_match
                           on  source.cleansed_first_name       = email_match.cleansed_first_name
                           and source.cleansed_last_name        = email_match.cleansed_last_name
                           and source.cleansed_name_suffix      = email_match.cleansed_name_suffix                     
      	                   and source.cleansed_email            = email_match.cleansed_email
      	                   and source.cleansed_first_name      != ''
      	                   and source.cleansed_last_name       != ''
                           and source.cleansed_name_suffix     != ''               
      	                   and source.cleansed_email           != ''     
"""
customer_work_unmatched_1 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_1` as
with member_guid as
(select
GENERATE_UUID() as customer_adw_key,
member_ky
from (select distinct member_ky from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_matched` where customer_adw_key is null)
) 
select
b.customer_adw_key as customer_adw_key,
original.membership_ky,
original.member_ky,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.customer_id,
original.first_name,
original.middle_name,
original.last_name,
original.name_suffix,
original.salutation,
original.address_line1,
original.address_line2,
original.city,
original.state,
original.zip,
original.zip_4,
original.country,
original.phone,
original.email,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_name,
original.cleansed_last_name,
original.cleansed_name_suffix,                      
original.cleansed_address_line1,
original.cleansed_city,
original.cleansed_state,
original.cleansed_zip,
original.cleansed_email        
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_matched` original
left outer join member_guid b on original.member_ky=b.member_ky
where original.customer_adw_key is null
"""

customer_work_unmatched_2 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_2` as
with address_guid as
(
select 
GENERATE_UUID() as customer_adw_key,
                   cleansed_first_name, 
                   cleansed_last_name,
                   cleansed_name_suffix        
                   cleansed_address_line1,
                   cleansed_city,
                   cleansed_state,
                   cleansed_zip
               from (select distinct 
                            cleansed_first_name
                           ,cleansed_last_name
                           ,cleansed_name_suffix      
                           ,cleansed_address_line1
                           ,cleansed_city
                           ,cleansed_state
                           ,cleansed_zip
                      from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_1`
                  where cleansed_first_name     != ''                          
           	        and cleansed_last_name      != ''       
           	        and cleansed_name_suffix    != ''                                                
           	        and cleansed_address_line1  != ''                           
           	        and cleansed_city           != ''                          
           	        and cleansed_state          != ''                          
           	        and cleansed_zip            != ''
                   )
           ),
matched_guid as
           (
                 select distinct
                        a.customer_adw_key as orig_customer_adw_key,
                        min(b.customer_adw_key) as new_customer_adw_key
                   from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_1` a
                        join address_guid b 
                            on 
                                a.cleansed_first_name     = b.cleansed_first_name
                 	         and a.cleansed_last_name      = b.cleansed_last_name
                 	         and a.cleansed_name_suffix    = b.cleansed_name_suffix                     
                 	         and a.cleansed_address_line1  = b.cleansed_address_line1
                 	         and a.cleansed_city           = b.cleansed_city
                 	         and a.cleansed_state          = b.cleansed_state
                 	         and a.cleansed_zip            = b.cleansed_zip
                 group by 
                            a.customer_adw_key
           )
select 
distinct
coalesce(matched.new_customer_adw_key,customer_adw_key) as customer_adw_key,
original.membership_ky,
original.member_ky,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.customer_id,
original.first_name,
original.middle_name,
original.last_name,
original.name_suffix,
original.salutation,
original.address_line1,
original.address_line2,
original.city,
original.state,
original.zip,
original.zip_4,
original.country,
original.phone,
original.email,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_name,
original.cleansed_last_name,
original.cleansed_name_suffix,                      
original.cleansed_address_line1,
original.cleansed_city,
original.cleansed_state,
original.cleansed_zip,
original.cleansed_email     
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_1` original
left outer join matched_guid matched on original.customer_adw_key=matched.orig_customer_adw_key

"""
customer_work_unmatched_3 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_3` as

with email_guid as
(
    select 
        GENERATE_UUID() as customer_adw_key ,
        first_name, 
        last_name,
        email
        from (select distinct 
                     cleansed_first_name
                    ,cleansed_last_name
                    ,cleansed_name_suffix      
                    ,cleansed_email
               from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_2`
             where cleansed_first_name     != ''                          
    	        and cleansed_last_name      != ''       
    	        and cleansed_name_suffix    != ''         
    	        and cleansed_email          != ''                         
           )
),

matched_guid as
(
     select distinct
         a.customer_adw_key  as orig_customer_adw_key ,
         min(b.customer_adw_key) as new_customer_adw_key 
         from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_2` a
     join email_guid b 
       on a.cleansed_first_name = b.cleansed_first_name
      and a.cleansed_last_name  = b.cleansed_last_name
      and a.cleansed_email      = b.cleansed_email
     group by a.customer_adw_key 
)

select 
coalesce(matched.new_customer_adw_key ,customer_adw_key ) as customer_adw_key ,
original.membership_ky,
original.member_ky,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.customer_id,
original.first_name,
original.middle_name,
original.last_name,
original.name_suffix,
original.salutation,
original.address_line1,
original.address_line2,
original.city,
original.state,
original.zip,
original.country,
original.phone,
original.email,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_name,
original.cleansed_last_name,
original.cleansed_name_suffix,                      
original.cleansed_address_line1,
original.cleansed_city,
original.cleansed_state,
original.cleansed_zip,
original.cleansed_email      

from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_2` original

left outer join matched_guid matched on original.customer_adw_key = matched.orig_customer_adw_key 
"""
customer_work_final_staging = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage` as 

select 
customer_adw_key ,
membership_ky,
member_ky,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
customer_id,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_name`(first_name, middle_name, last_name, name_suffix, salutation) as name_adw_key,
first_name,
middle_name,
last_name,
name_suffix,
salutation,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_address`(address_line1, address_line2, city, state, zip, country) as address_adw_key,
address_line1,
address_line2,
city,
state,
zip,
country,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_phone`(phone) as phone_adw_key,
phone,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_email`(email) as email_adw_key,
email,
gender,
birthdate,
last_upd_dt,
cleansed_first_name,
cleansed_last_name,
cleansed_name_suffix,                      
cleansed_address_line1,
cleansed_city,
cleansed_state,
cleansed_zip,
cleansed_email
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_matched`
where customer_adw_key is not null

union all 

select 
customer_adw_key ,
membership_ky,
member_ky,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
customer_id,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_name`(first_name, middle_name, last_name, name_suffix, salutation) as name_adw_key,
first_name,
middle_name,
last_name,
name_suffix,
salutation,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_address`(address_line1, address_line2, city, state, zip, country) as address_adw_key,
address_line1,
address_line2,
city,
state,
zip,
country,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_phone`(phone) as phone_adw_key,
phone,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_email`(email) as email_adw_key,
email,
gender,
birthdate,
last_upd_dt,
cleansed_first_name,
cleansed_last_name,
cleansed_name_suffix,                      
cleansed_address_line1,
cleansed_city,
cleansed_state,
cleansed_zip,
cleansed_email

from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_unmatched_work_3`
"""

#################################
# Insert into static dimensions #
#################################
# NAME
query_name_insert = f"""insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name`
(name_adw_key ,    
name_first_name,
name_middle_name,
name_last_name,    
name_suffix_name,
name_title_name,
integrate_insert_datetime,
integrate_insert_batch_number
)
select
distinct
name_adw_key ,
first_name,
NULLIF(middle_name, '') as middle_name,
last_name,
name_suffix,
salutation,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
where name_adw_key not in (select distinct name_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name`)"""
query_name_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_name_stage` as
with combined_cust_name as
(
select customer_adw_key,
name_adw_key,
cast(last_upd_dt as date) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
union distinct
select
target.customer_adw_key,
target.name_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_name` target
where customer_source_system_name='mzp'
 and name_type_code='membership'
),
cust_name as
(select
customer_adw_key,
name_adw_key,
cast(last_upd_dt as date) as effective_start_datetime,
lag(name_adw_key) over (partition by customer_adw_key order by cast(last_upd_dt as date)) as prev_name,
coalesce(lead(cast(last_upd_dt as date)) over (partition by customer_adw_key order by cast(last_upd_dt as date)), date('9999-12-31'
) )as next_record_date
from combined_cust_name
),
set_grouping_column as
(select
customer_adw_key,
name_adw_key,
case when prev_name is null or prev_name<>name_adw_key then 1 else 0 end as new_name_tag,
effective_start_datetime,
next_record_date
from cust_name
), set_groups as
(select
customer_adw_key,
name_adw_key,
sum(new_name_tag) over (partition by customer_adw_key order by effective_start_datetime) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
customer_adw_key,
name_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
customer_adw_key,
name_adw_key,
grouping_column
),
update_key_type_name as
(
select
customer_adw_key,
name_adw_key,
effective_start_datetime,
effective_end_datetime,
row_number() over(partition by customer_adw_key,  effective_start_datetime order by effective_end_datetime desc,name_adw_key ) as rn
from deduped
)
select
customer_adw_key,
name_adw_key,
'mzp' as source_system,
concat('membership', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as name_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
1 as batch_insert,
current_datetime update_datetime,
1 as batch_update
from update_key_type_name
"""
query_name_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_name` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_name_stage` source
on target.customer_adw_key=source.customer_adw_key and 
   target.customer_source_system_name=source.source_system and
   target.name_type_code=source.name_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as date) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
customer_adw_key,	
name_adw_key,	
customer_source_system_name,
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
source.customer_adw_key,
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
when not matched by source then delete
"""

# ADDRESS
query_address_insert = f"""insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address`
(address_adw_key ,    
address_1_name,    
address_2_name,    
address_care_of_name,    
address_city_name,    
address_state_code,    
address_state_name,    
address_postal_code,    
address_postal_plus_4_code,    
address_country_name,    
integrate_insert_datetime,    
integrate_insert_batch_number
)
select
distinct 
address_adw_key ,
address_line1,
NULLIF(address_line2, '') as address_line2,
cast(null as string) care_of_name,
city,
state as state_code,
cast(null as string) state_name,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_zipcode`(zip, 'prefix') as postal_code,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_zipcode`(zip, 'suffix') postal_code_plus_4,
country,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
where address_adw_key not in (select distinct address_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address`)
"""
query_address_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_address_stage` as
with combined_cust_address as
(
select customer_adw_key,
address_adw_key,
cast(last_upd_dt as date) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
union distinct
select
target.customer_adw_key,
target.address_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_address` target
where customer_source_system_name='mzp'
 and address_type_code='membership'
),
cust_address as
(select
customer_adw_key,
address_adw_key,
cast(last_upd_dt as date) as effective_start_datetime,
lag(address_adw_key) over (partition by customer_adw_key order by cast(last_upd_dt as date)) as prev_address,
coalesce(lead(cast(last_upd_dt as date)) over (partition by customer_adw_key order by cast(last_upd_dt as date)), date('9999-12-31'
) )as next_record_date
from combined_cust_address
),
set_grouping_column as
(select
customer_adw_key,
address_adw_key,
case when prev_address is null or prev_address<>address_adw_key then 1 else 0 end as new_address_tag,
effective_start_datetime,
next_record_date
from cust_address
), set_groups as
(select
customer_adw_key,
address_adw_key,
sum(new_address_tag) over (partition by customer_adw_key order by effective_start_datetime) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
customer_adw_key,
address_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
customer_adw_key,
address_adw_key,
grouping_column
),

update_key_type_name as
(
select
customer_adw_key,
address_adw_key,
effective_start_datetime,
effective_end_datetime,
row_number() over(partition by customer_adw_key,  effective_start_datetime order by effective_end_datetime desc,address_adw_key ) as rn
from deduped
)

select
customer_adw_key,
address_adw_key,
'mzp' as source_system,
concat('membership', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as address_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
1 as batch_insert,
current_datetime update_datetime,
1 as batch_update
from update_key_type_name

"""
query_address_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_address` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_address_stage` source
on target.customer_adw_key=source.customer_adw_key and 
   target.customer_source_system_name=source.source_system and
   target.address_type_code=source.address_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as date) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
customer_adw_key,	
address_adw_key,	
customer_source_system_name,
address_type_code,	
effective_start_datetime,	
effective_end_datetime,	
integrate_insert_datetime,	
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.customer_adw_key,
source.address_adw_key,
source.source_system,
source.address_type,
source.effective_start_datetime,
source.effective_end_datetime,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source then delete
"""

# PHONE
query_phone_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone`
(phone_adw_key ,    
phone_number,
phone_formatted_number,    
phone_extension_number,    
integrate_insert_datetime,    
integrate_insert_batch_number
)
select
distinct
phone_adw_key ,
phone,
`{int_u.INTEGRATION_PROJECT}.udfs.format_phone_number`(phone) as formatted_phone,
cast(null as string) as customer_phone_ext,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
where phone_adw_key not in (select distinct phone_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone`)
and phone is not null"""
query_phone_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_phone_stage` as
with combined_cust_phone as
(
select customer_adw_key,
phone_adw_key,
cast(last_upd_dt as date) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
union distinct
select
target.customer_adw_key,
target.phone_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_phone` target
where customer_source_system_name='mzp'
 and phone_type_code='membership'
),
cust_phone as
(select
customer_adw_key,
phone_adw_key,
cast(last_upd_dt as date) as effective_start_datetime,
lag(phone_adw_key) over (partition by customer_adw_key order by cast(last_upd_dt as date)) as prev_phone,
coalesce(lead(cast(last_upd_dt as date)) over (partition by customer_adw_key order by cast(last_upd_dt as date)), date('9999-12-31'
) )as next_record_date
from combined_cust_phone
),
set_grouping_column as
(select
customer_adw_key,
phone_adw_key,
case when prev_phone is null or prev_phone<>phone_adw_key then 1 else 0 end as new_phone_tag,
effective_start_datetime,
next_record_date
from cust_phone
), set_groups as
(select
customer_adw_key,
phone_adw_key,
sum(new_phone_tag) over (partition by customer_adw_key order by effective_start_datetime) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
customer_adw_key,
phone_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
customer_adw_key,
phone_adw_key,
grouping_column
),
update_key_type_name as
(
select
customer_adw_key,
phone_adw_key,
effective_start_datetime,
effective_end_datetime,
row_number() over(partition by customer_adw_key,  effective_start_datetime order by effective_end_datetime desc,phone_adw_key ) as rn
from deduped
)
select
customer_adw_key,
phone_adw_key,
'mzp' as source_system,
concat('membership', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as phone_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
1 as batch_insert,
current_datetime update_datetime,
1 as batch_update
from update_key_type_name
"""
query_phone_merge = f"""

merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_phone` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_phone_stage` source
on target.customer_adw_key=source.customer_adw_key and 
   target.customer_source_system_name=source.source_system and
   target.phone_type_code=source.phone_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as date) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
customer_adw_key,	
phone_adw_key,	
customer_source_system_name,
phone_type_code,	
effective_start_datetime,	
effective_end_datetime,	
integrate_insert_datetime,	
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.customer_adw_key,
source.phone_adw_key,
source.source_system,
source.phone_type,
source.effective_start_datetime,
source.effective_end_datetime,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source then delete
"""

# EMAIL
query_email_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email`
(email_adw_key ,
email_name,    
integrate_insert_datetime,
integrate_insert_batch_number
)
select
distinct
email_adw_key ,
email,
current_datetime,
1 as batch
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
where email_adw_key not in (select distinct email_adw_key  from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email`)
and email is not null and email != ''
"""
query_email_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_email_stage` as
with combined_cust_email as
(
select customer_adw_key,
email_adw_key,
cast(last_upd_dt as date) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
union distinct
select
target.customer_adw_key,
target.email_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_email` target
where customer_source_system_name='mzp'
 and email_type_code='membership'
),
cust_email as
(select
customer_adw_key,
email_adw_key,
cast(last_upd_dt as date) as effective_start_datetime,
lag(email_adw_key) over (partition by customer_adw_key order by cast(last_upd_dt as date), email_adw_key) as prev_email,
coalesce(lead(cast(last_upd_dt as date)) over (partition by customer_adw_key order by cast(last_upd_dt as date), email_adw_key), date('9999-12-31'
) )as next_record_date
from combined_cust_email
),
set_grouping_column as
(select
customer_adw_key,
email_adw_key,
case when prev_email is null or prev_email<>email_adw_key then 1 else 0 end as new_email_tag,
effective_start_datetime,
next_record_date
from cust_email
), set_groups as
(select
customer_adw_key,
email_adw_key,
sum(new_email_tag) over (partition by customer_adw_key order by effective_start_datetime, email_adw_key) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
customer_adw_key,
email_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
customer_adw_key,
email_adw_key,
grouping_column
), update_key_type_name as
(
select
customer_adw_key,
email_adw_key,
effective_start_datetime,
effective_end_datetime,
row_number() over(partition by customer_adw_key,  effective_start_datetime order by effective_end_datetime desc,email_adw_key ) as rn
from deduped
)
select
customer_adw_key,
email_adw_key,
'mzp' as source_system,
concat('membership', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as email_type,
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
merge into `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_email` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_email_stage` source
on target.customer_adw_key=source.customer_adw_key and 
   target.customer_source_system_name=source.source_system and
   target.email_type_code=source.email_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as date) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
customer_adw_key,	
email_adw_key,	
customer_source_system_name,
email_type_code,	
effective_start_datetime,	
effective_end_datetime,	
integrate_insert_datetime,	
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.customer_adw_key,
source.email_adw_key,
source.source_system,
source.email_type,
source.effective_start_datetime,
source.effective_end_datetime,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source then delete
"""

# KEYS
query_source_key = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_source_key_stage` AS
WITH
  normalized_source_key AS (
  SELECT
    customer_adw_key,
    'membership_key' AS customer_key_type_name,
    CAST(last_upd_dt AS date) last_upd_dt,
    membership_ky AS customer_source_1_key,
    CAST(NULL AS string) AS customer_source_2_key,
    CAST(NULL AS string) AS customer_source_3_key,
    CAST(NULL AS string) AS customer_source_4_key,
    CAST(NULL AS string) AS customer_source_5_key,
    membership_ky AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
  UNION DISTINCT
  SELECT
    customer_adw_key,
    'member_key' AS customer_key_type_name,
    CAST(last_upd_dt AS date) last_upd_dt,
    member_ky AS customer_source_1_key,
    CAST(NULL AS string) AS customer_source_2_key,
    CAST(NULL AS string) AS customer_source_3_key,
    CAST(NULL AS string) AS customer_source_4_key,
    CAST(NULL AS string) AS customer_source_5_key,
    member_ky AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
  UNION DISTINCT
  SELECT
    customer_adw_key,
    'member_id' AS customer_key_type_name,
    CAST(last_upd_dt AS date) last_upd_dt,
    CAST(iso_cd AS string) AS customer_source_1_key,
    club_cd AS customer_source_2_key,
    membership_id AS customer_source_3_key,
    associate_id AS customer_source_4_key,
    check_digit_nr AS customer_source_5_key,
    CONCAT(ifnull(CAST(iso_cd AS string),
        ''),ifnull(club_cd,
        ''),ifnull(membership_id,
        ''),ifnull(associate_id,
        ''),ifnull(check_digit_nr,
        '')) AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
  UNION DISTINCT
  SELECT
    customer_adw_key,
    'customer_id' AS customer_key_type_name,
    CAST(last_upd_dt AS date) last_upd_dt,
    customer_id AS customer_source_1_key,
    CAST(NULL AS string) AS customer_source_2_key,
    CAST(NULL AS string) AS customer_source_3_key,
    CAST(NULL AS string) AS customer_source_4_key,
    CAST(NULL AS string) AS customer_source_5_key,
    customer_id AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage` ),
  all_keys AS (
  SELECT
    customer_adw_key,
    case 
        when customer_key_type_name like 'member_id%' then 'member_id'
        when customer_key_type_name like 'membership_key%' then 'membership_key'
        when customer_key_type_name like 'member_key%' then 'member_key'
    else 'customer_id'
  END
    AS customer_key_type_name,
    effective_start_datetime AS last_upd_dt,
    customer_source_1_key,
    customer_source_2_key,
    customer_source_3_key,
    customer_source_4_key,
    customer_source_5_key,
    CASE
      WHEN customer_key_type_name like 'member_id%' THEN CONCAT(ifnull(CAST(customer_source_1_key AS string), ''), ifnull(customer_source_2_key, ''), ifnull(customer_source_3_key, ''), ifnull(customer_source_4_key, ''), ifnull(customer_source_5_key, ''))
    ELSE
    customer_source_1_key
  END
    AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_customer_source_key`
  WHERE
    customer_source_system_name = 'mzp'
  UNION DISTINCT
  SELECT
    *
  FROM
    normalized_source_key ),
  cust_keys AS (
  SELECT
    customer_adw_key,
    customer_key_type_name,
    last_upd_dt AS effective_start_datetime,
    LAG(dedupe_check) OVER (PARTITION BY customer_adw_key, customer_key_type_name ORDER BY last_upd_dt, dedupe_check) AS prev_dedupe_check,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY customer_adw_key, customer_key_type_name ORDER BY last_upd_dt, dedupe_check),
      DATE('9999-12-31' ) )AS next_record_date,
    customer_source_1_key,
    customer_source_2_key,
    customer_source_3_key,
    customer_source_4_key,
    customer_source_5_key,
    dedupe_check
  FROM
    all_keys ),
  set_grouping_column AS (
  SELECT
    customer_adw_key,
    customer_key_type_name,
    CASE
      WHEN dedupe_check IS NULL OR prev_dedupe_check<>dedupe_check THEN 1
    ELSE
    0
  END
    AS new_key_tag,
    effective_start_datetime,
    next_record_date,
    customer_source_1_key,
    customer_source_2_key,
    customer_source_3_key,
    customer_source_4_key,
    customer_source_5_key,
    dedupe_check
  FROM
    cust_keys ),
  set_groups AS (
  SELECT
    customer_adw_key,
    customer_key_type_name,
    SUM(new_key_tag) OVER (PARTITION BY customer_adw_key, customer_key_type_name ORDER BY effective_start_datetime, dedupe_check) AS grouping_column,
    effective_start_datetime,
    next_record_date,
    customer_source_1_key,
    customer_source_2_key,
    customer_source_3_key,
    customer_source_4_key,
    customer_source_5_key
  FROM
    set_grouping_column ),
  deduped AS (
  SELECT
    customer_adw_key,
    customer_key_type_name,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_date) AS effective_end_datetime,
    customer_source_1_key,
    customer_source_2_key,
    customer_source_3_key,
    customer_source_4_key,
    customer_source_5_key
  FROM
    set_groups
  GROUP BY
    customer_adw_key,
    customer_key_type_name,
    grouping_column,
    customer_source_1_key,
    customer_source_2_key,
    customer_source_3_key,
    customer_source_4_key,
    customer_source_5_key ),
  update_key_type_name AS (
  SELECT
    customer_adw_key,
    customer_key_type_name,
    effective_start_datetime,
    effective_end_datetime,
    customer_source_1_key,
    customer_source_2_key,
    customer_source_3_key,
    customer_source_4_key,
    customer_source_5_key,
    ROW_NUMBER() OVER(PARTITION BY customer_adw_key, customer_key_type_name, effective_start_datetime ORDER BY effective_end_datetime DESC, customer_source_1_key, customer_source_2_key, customer_source_3_key, customer_source_4_key, customer_source_5_key) AS rn
  FROM
    deduped )
SELECT
  customer_adw_key,
  'mzp' AS customer_source_system_name,
  CASE
    WHEN rn=1 THEN customer_key_type_name
  ELSE
  CONCAT(customer_key_type_name,'-',CAST(rn AS string))
END
  AS customer_key_type_name,
  effective_start_datetime,
  effective_end_datetime,
  customer_source_1_key,
  customer_source_2_key,
  customer_source_3_key,
  customer_source_4_key,
  customer_source_5_key,
  current_datetime insert_datetime,
  1 AS batch_insert,
  current_datetime update_datetime,
  1 AS batch_update
FROM
  update_key_type_name
"""
query_source_key_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw.dim_customer_source_key` target
using `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_source_key_stage` source
on target.customer_adw_key=source.customer_adw_key and 
   target.customer_source_system_name=source.customer_source_system_name and
   target.customer_key_type_name=source.customer_key_type_name and
   target.effective_start_datetime=source.effective_start_datetime
when matched and target.effective_end_datetime=cast('9999-12-31' as date) then update
set 
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update
when not matched by target then insert
(
customer_adw_key,
customer_source_system_name,
customer_key_type_name,
effective_start_datetime,
effective_end_datetime,
customer_source_1_key,
customer_source_2_key,
customer_source_3_key,
customer_source_4_key,
customer_source_5_key,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.customer_adw_key,
source.customer_source_system_name,
source.customer_key_type_name,
source.effective_start_datetime,
source.effective_end_datetime,
source.customer_source_1_key,
source.customer_source_2_key,
source.customer_source_3_key,
source.customer_source_4_key,
source.customer_source_5_key,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source then delete
"""

# CUSTOMER
query_customer_insert = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_customer`
(customer_adw_key ,    
customer_gender_code,    
customer_gender_name,    
customer_birth_date,
integrate_insert_datetime,    
integrate_insert_batch_number,    
integrate_update_datetime,    
integrate_update_batch_number)

select
customer_adw_key ,
max(gender) as gender_code,
cast(null as string) as gender_name,
max(birthdate) as birth_date,
current_datetime,
1 as batch_insert,
current_datetime,
1 as batch_update
from `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_customer_stage`
where customer_adw_key not in (select distinct customer_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_customer`)
group by customer_adw_key """
query_customer_merge = f"""
merge into `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_customer` target
using
(
select 
customer.customer_adw_key as customer_adw_key,
name.name_first_name first_name,
name.name_middle_name middle_name,
name.name_last_name last_name,
name.name_suffix_name name_suffix,
name.name_title_name salutation,
address.address_1_name address_line1,
address.address_city_name city,
address.address_state_code state,
address.address_postal_code zip,
address.address_postal_plus_4_code	 zip_4,
address.address_country_name country,
phone.phone_number,
phone.phone_formatted_number,
phone.phone_extension_number,
email.email_name email,
customer.customer_birth_date birthdate
from
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_customer` customer
join 
(select
customer_adw_key,
customer_source_system_name,
row_number() over(partition by customer_adw_key order by case when customer_source_system_name='mzp' then 1 when customer_source_system_name='epic' then 2 when customer_source_system_name='d3' then 3 end asc, effective_end_datetime desc, effective_start_datetime desc) as rn
from `{int_u.INTEGRATION_PROJECT}.adw.dim_customer_source_key`
) primacy on customer.customer_adw_key=primacy.customer_adw_key	and primacy.rn=1

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_name` name_bridge 
    on customer.customer_adw_key=name_bridge.customer_adw_key and
	name_bridge.customer_source_system_name=primacy.customer_source_system_name and
	name_bridge.name_type_code='membership' and
	name_bridge.effective_end_datetime=date('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name 
    on name_bridge.name_adw_key=name.name_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_address` address_bridge 
    on customer.customer_adw_key=address_bridge.customer_adw_key and
	address_bridge.customer_source_system_name=primacy.customer_source_system_name and
	address_bridge.address_type_code='membership' and
	address_bridge.effective_end_datetime=date('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address` address 
    on address_bridge.address_adw_key=address.address_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_email` email_bridge 
    on customer.customer_adw_key=email_bridge.customer_adw_key and
	email_bridge.customer_source_system_name=primacy.customer_source_system_name and
	email_bridge.email_type_code='membership' and
	email_bridge.effective_end_datetime=date('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email` email 
    on email_bridge.email_adw_key=email.email_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_customer_phone` phone_bridge 
    on customer.customer_adw_key=phone_bridge.customer_adw_key and
	phone_bridge.customer_source_system_name=primacy.customer_source_system_name and
	phone_bridge.phone_type_code='membership' and
	phone_bridge.effective_end_datetime=date('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone` phone 
    on phone_bridge.phone_adw_key=phone.phone_adw_key

) source
on source.customer_adw_key=target.customer_adw_key
when matched then update --update this later to only update if something has changed
set 
target.customer_first_name=source.first_name,
target.customer_middle_name=source.middle_name,
target.customer_last_name=source.last_name,
target.customer_name_suffix_name=source.name_suffix,
target.customer_title_name=source.salutation,
target.customer_address_1_name=source.address_line1,
target.customer_city_name=source.city,
target.customer_state_code=source.state,
target.customer_postal_code=source.zip,
target.customer_postal_plus_4_code=source.zip_4,
target.customer_country_name=source.country,
target.customer_email_name=source.email,
target.customer_birth_date=source.birthdate,
target.customer_phone_number=source.phone_number,
target.customer_phone_formatted_number=source.phone_formatted_number,
target.customer_phone_extension_number=source.phone_extension_number
"""

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    # Matching Logic
    task_customer_work_source = int_u.run_query("build_customer_work_source", customer_work_source)
    task_customer_work_target = int_u.run_query("build_customer_work_target", customer_work_target)
    task_customer_work_matched = int_u.run_query("build_customer_work_matched", customer_work_matched)
    task_customer_work_unmatched_1 = int_u.run_query("build_customer_work_unmatched_1", customer_work_unmatched_1)
    task_customer_work_unmatched_2 = int_u.run_query("build_customer_work_unmatched_2", customer_work_unmatched_2)
    task_customer_work_unmatched_3 = int_u.run_query("build_customer_work_unmatched_3", customer_work_unmatched_3)
    task_customer_work_final_staging = int_u.run_query("build_customer_work_final_staging", customer_work_final_staging)

    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_name', query_name_insert)
    task_name_bridge = int_u.run_query('build_name_bridge', query_name_bridge, 2)
    task_name_merge = int_u.run_query('merge_name', query_name_merge)

    ## address
    task_address_insert = int_u.run_query('insert_address', query_address_insert)
    task_address_bridge = int_u.run_query('build_address_bridge', query_address_bridge, 2)
    task_address_merge = int_u.run_query('merge_address', query_address_merge)

    ## phone
    task_phone_insert = int_u.run_query('insert_phone', query_phone_insert)
    task_phone_bridge = int_u.run_query('build_phone_bridge', query_phone_bridge, 2)
    task_phone_merge = int_u.run_query('merge_phone', query_phone_merge)

    ## email
    task_email_insert = int_u.run_query('insert_email', query_email_insert)
    task_email_bridge = int_u.run_query('build_email_bridge', query_email_bridge, 2)
    task_email_merge = int_u.run_query('merge_email', query_email_merge)

    ## keys
    task_keys = int_u.run_query('build_source_keys', query_source_key)
    task_keys_bridge = int_u.run_query('merge_keys', query_source_key_merge)

    # customer
    task_customer_insert = int_u.run_query('insert_customer', query_customer_insert)
    task_customer_merge = int_u.run_query('merge_customer', query_customer_merge)

    ######################################################################
    # DEPENDENCIES

    task_customer_work_source >> task_customer_work_matched
    task_customer_work_target >> task_customer_work_matched

    task_customer_work_matched >> \
    task_customer_work_unmatched_1 >> \
    task_customer_work_unmatched_2 >> \
    task_customer_work_unmatched_3 >> \
    task_customer_work_final_staging

    task_customer_work_final_staging >> task_name_insert >> task_customer_insert
    task_customer_work_final_staging >> task_name_bridge >> task_name_merge >> task_customer_insert

    task_customer_work_final_staging >> task_address_insert >> task_customer_insert
    task_customer_work_final_staging >> task_address_bridge >> task_address_merge >> task_customer_insert

    task_customer_work_final_staging >> task_phone_insert >> task_customer_insert
    task_customer_work_final_staging >> task_phone_bridge >> task_phone_merge >> task_customer_insert

    task_customer_work_final_staging >> task_email_insert >> task_customer_insert
    task_customer_work_final_staging >> task_email_bridge >> task_email_merge >> task_customer_insert

    task_customer_work_final_staging >> task_keys >> task_keys_bridge >> task_customer_insert >> task_customer_merge
