from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_contact_epic"

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
contact_work_source = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_work` as
select
`{int_u.INTEGRATION_PROJECT}.udfs.parse_member_id`(contactnumber.descriptionof,'iso_cd', contactnumber.descriptionof,contactnumber.descriptionof) iso_cd,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_member_id`(contactnumber.descriptionof,'club_cd', contactnumber.descriptionof,contactnumber.descriptionof) club_cd,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_member_id`(contactnumber.descriptionof,'membership_id', contactnumber.descriptionof,contactnumber.descriptionof) membership_id,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_member_id`(contactnumber.descriptionof,'associate_id', contactnumber.descriptionof,contactnumber.descriptionof) associate_id,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_member_id`(contactnumber.descriptionof,'check_digit_nr', contactnumber.descriptionof,contactnumber.descriptionof) check_digit_nr,

client.uniqentity as client_unique_entity,
coalesce(trim(contactname.firstname),'') as raw_first_name,
coalesce(trim(contactname.middlename),'') as raw_middle_name,
coalesce(trim(contactname.lastname),'') as raw_last_name,
coalesce(trim(contactname.lksuffix),'') as raw_suffix_name,
coalesce(trim(contactname.lkprefix),'') as raw_title_name,
coalesce(trim(contactaddress.address1),'') as raw_address_1_name,
coalesce(trim(contactaddress.address2),'') as raw_address_2_name,
cast('' as string) as raw_care_of_name,
coalesce(trim(contactaddress.city),'') as raw_city_name,
coalesce(trim(contactaddress.cdstatecode),'') as raw_state_code,
coalesce(trim(contactaddress.postalcode),'') as raw_postal_code,
'' as raw_postal_plus_4_code,
coalesce(trim(contactaddress.cdcountrycode),'') as raw_country_name,
coalesce(trim(contactnumber.number),'') as raw_phone_number,
coalesce(trim(contactnumber.emailweb),'') as raw_email_name,
coalesce(trim(contactname.gendercode),'') as gender,
safe_cast(substr(contactname.birthdate,1,10) as date) as birthdate,
--`{int_u.INTEGRATION_PROJECT}.udfs.parse_zipcode`(trim(contactaddress.postalcode), 'prefix') as zip,
--`{int_u.INTEGRATION_PROJECT}.udfs.parse_zipcode`(trim(contactaddress.postalcode),'suffix') as zip_4,

greatest(
coalesce(cast(substr(client.updateddate, 1, 23) as datetime),datetime('2000-01-01')), 
coalesce(cast(substr(contactname.updateddate, 1, 23) as datetime),datetime('2000-01-01')),
coalesce(cast(substr(contactaddress.updateddate, 1, 23) as datetime),datetime('2000-01-01')),
coalesce(cast(substr(contactnumber.updateddate, 1, 23) as datetime),datetime('2000-01-01'))
) as last_upd_dt,
           
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(contactname.firstname))))                                as cleansed_first_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(contactname.middlename))))                                as cleansed_middle_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(contactname.lastname))))                                 as cleansed_last_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(contactname.lksuffix))))                               as cleansed_suffix_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_numeric_suffix_to_alpha`(
    `{int_u.INTEGRATION_PROJECT}.udfs.udf_diacritic_to_standard`(
        `{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
            trim(contactname.lkprefix))))                               as cleansed_title_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    lower(trim(contactaddress.address1)))                                  as cleansed_address_1_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    lower(trim(contactaddress.address2)))                                  as cleansed_address_2_name,
cast(null as string)                                                 as cleansed_care_of_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    lower(trim(contactaddress.city)))                                           as cleansed_city_name,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    lower(trim(contactaddress.cdstatecode)))                                          as cleansed_state_code,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_zipcode`(trim(contactaddress.postalcode), 'prefix')                                           as cleansed_postal_code,
`{int_u.INTEGRATION_PROJECT}.udfs.parse_zipcode`(trim(contactaddress.postalcode),'suffix')                                 as cleansed_postal_plus_4_code,
`{int_u.INTEGRATION_PROJECT}.udfs.udf_remove_selected_punctuation`(
    lower(trim(contactaddress.cdcountrycode)))                                        as cleansed_country_name,
coalesce(lower(trim(contactnumber.emailweb)),'')                                      as cleansed_email_name,
`{int_u.INTEGRATION_PROJECT}.udfs.format_phone_number`(
    coalesce(trim(contactnumber.number),''))                                  as cleansed_phone_number
from 
     (select 
      *,
	  row_number() over (partition by uniqentity order by updateddate desc) as latest_record_check
      from `adw-lake-dev.epic.client` where uniqentity<>'-1') client 
left join (select 
      *,
	  row_number() over (partition by uniqcontactname order by updateddate desc) as latest_record_check	  
      from `adw-lake-dev.epic.contactname`) contactname on client.uniqcontactnameprimary=contactname.uniqcontactname and client.latest_record_check=1 and contactname.latest_record_check=1
left join (select 
      *,
	  row_number() over (partition by uniqcontactaddress order by updateddate desc) as latest_record_check
      from `adw-lake-dev.epic.contactaddress`) contactaddress on client.uniqcontactaddressaccount=contactaddress.uniqcontactaddress and contactaddress.latest_record_check=1
left join (select 
      *,
	  row_number() over (partition by uniqcontactnumber order by updateddate desc) as latest_record_check
      from `adw-lake-dev.epic.contactnumber`) contactnumber on client.uniqcontactnumberaccount=contactnumber.uniqcontactnumber and contactnumber.latest_record_check=1
where greatest(coalesce(cast(substr(client.updateddate, 1, 23) as datetime),datetime('2000-01-01')), 
               coalesce(cast(substr(contactname.updateddate, 1, 23) as datetime),datetime('2000-01-01')),
               coalesce(cast(substr(contactaddress.updateddate, 1, 23) as datetime),datetime('2000-01-01')),
               coalesce(cast(substr(contactnumber.updateddate, 1, 23) as datetime),datetime('2000-01-01'))
               ) > (select max(effective_start_datetime) from `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` where contact_source_system_name = 'epic' )
"""
contact_work_target = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work` as 

select 
contact.contact_adw_key as contact_adw_key,
epic_keys.source_1_key as client_unique_entity,
member_ids.source_1_key as iso_cd,
member_ids.source_2_key as club_cd,
member_ids.source_3_key as membership_id,
member_ids.source_4_key as associate_id,
member_ids.source_5_key as check_digit_nr, 

name.cleansed_first_name  as cleansed_first_name,
name.cleansed_last_name as cleansed_last_name,
name.cleansed_last_name as cleansed_suffix_name,
address.cleansed_address_1_name as cleansed_address_1_name,
address.cleansed_city_name as cleansed_city_name,
address.cleansed_state_code as cleansed_state_code,
address.cleansed_postal_code as cleansed_postal_code,
email.cleansed_email_name as cleansed_email_name,

member_ids.effective_start_datetime as effective_start_datetime,
member_ids.effective_end_datetime as effective_end_datetime,

count(distinct contact.contact_adw_key) over (
partition by member_ids.source_2_key,
member_ids.source_3_key,
member_ids.source_4_key
) as mbr_id_count

from
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info` contact

left join `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` epic_keys
	on contact.contact_adw_key=epic_keys.contact_adw_key and
    epic_keys.contact_source_system_name='epic'
    and epic_keys.key_type_name='uniqentity_key'

left join `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` member_ids
	on contact.contact_adw_key=member_ids.contact_adw_key and
	member_ids.key_type_name='member_id' -- all member ids regardless of source

left join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name` name_bridge 
  on contact.contact_adw_key=name_bridge.contact_adw_key 

join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name 
  on name_bridge.name_adw_key=name.name_adw_key

join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address` address_bridge 
  on contact.contact_adw_key=address_bridge.contact_adw_key 

join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address` address 
    on address_bridge.address_adw_key=address.address_adw_key	

left join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email` email_bridge
  on contact.contact_adw_key=email_bridge.contact_adw_key 

left join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email` email 
    on email_bridge.email_adw_key=email.email_adw_key

"""
contact_work_matched = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_matched` as
select
coalesce(uniqentity_match.contact_adw_key,
        member_id_match.contact_adw_key,
        address_match.contact_adw_key,
		zip5_match.contact_adw_key,
        email_match.contact_adw_key) as contact_adw_key,

source.iso_cd,
source.club_cd,
source.membership_id,
source.associate_id,
source.check_digit_nr,

source.client_unique_entity,
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

from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_work` source

      -- Matching Criteria 1: Client Unique Entity --       
left outer join (select max(contact_adw_key) as contact_adw_key
                      , client_unique_entity
                      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
					  where coalesce(client_unique_entity, '') != ''
                      group by client_unique_entity
                      ) uniqentity_match on source.client_unique_entity=uniqentity_match.client_unique_entity

      -- Matching Criteria 2: Member ID (club_cd, membership_id, associate_id) and last_upd_dt between effective_dates --     
left outer join (select max(contact_adw_key) as contact_adw_key
                      , club_cd
                      , membership_id
                      , associate_id
                      , effective_start_datetime
                      , effective_end_datetime
                      , mbr_id_count
                      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
					  where coalesce(membership_id, '') != ''
                      group by
                      club_cd
                      , membership_id
                      , associate_id
                      , effective_start_datetime
                      , effective_end_datetime
                      , mbr_id_count
                      ) member_id_match

    on coalesce(source.club_cd, '')    = coalesce(member_id_match.club_cd, '') and 
	coalesce(source.membership_id, '') = coalesce(member_id_match.membership_id, '') and 
	coalesce(source.associate_id, '')  = coalesce(member_id_match.associate_id, '') and 
    coalesce(source.membership_id, '') != '' and
    ((cast(last_upd_dt as datetime) between effective_start_datetime and effective_end_datetime) or (mbr_id_count = 1))

      -- Matching Criteria 3: First Name, Last Name, Address Line 1, City, State, Name Suffix --            
     LEFT OUTER JOIN (
       SELECT
         MAX(contact_adw_key) AS contact_adw_key
         , cleansed_first_name
         , cleansed_last_name
         , cleansed_suffix_name                      
         , cleansed_address_1_name
         , cleansed_city_name
         , cleansed_state_code
       FROM
         `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
		 WHERE coalesce(cleansed_city_name, '') != ''
       GROUP BY
           cleansed_first_name
         , cleansed_last_name
         , cleansed_suffix_name                      
         , cleansed_address_1_name
         , cleansed_city_name
         , cleansed_state_code ) address_match
       ON
            source.cleansed_first_name       = address_match.cleansed_first_name
        and source.cleansed_last_name        = address_match.cleansed_last_name
        and source.cleansed_suffix_name      = address_match.cleansed_suffix_name                     
        and source.cleansed_address_1_name    = address_match.cleansed_address_1_name
        and source.cleansed_city_name             = address_match.cleansed_city_name
        and source.cleansed_state_code            = address_match.cleansed_state_code    
        and source.cleansed_first_name      != ''
        and source.cleansed_last_name       != ''                  
        and source.cleansed_address_1_name   != ''
        and source.cleansed_city_name            != ''
        and source.cleansed_state_code           != ''        
	
      -- Matching Criteria 4: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --        

     LEFT OUTER JOIN (
       SELECT
         MAX(contact_adw_key) AS contact_adw_key
         , cleansed_first_name
         , cleansed_last_name
         , cleansed_suffix_name                      
         , cleansed_address_1_name
         , cleansed_postal_code
       FROM
         `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
		 WHERE coalesce(cleansed_postal_code, '') != ''
       GROUP BY
           cleansed_first_name
         , cleansed_last_name
         , cleansed_suffix_name                      
         , cleansed_address_1_name
         , cleansed_postal_code ) zip5_match
       ON
           source.cleansed_first_name       = zip5_match.cleansed_first_name
       and source.cleansed_last_name        = zip5_match.cleansed_last_name
       and source.cleansed_suffix_name      = zip5_match.cleansed_suffix_name                     
       and source.cleansed_address_1_name    = zip5_match.cleansed_address_1_name
       and source.cleansed_postal_code              = zip5_match.cleansed_postal_code
       and source.cleansed_first_name      != ''
       and source.cleansed_last_name       != ''                  
       and source.cleansed_address_1_name   != ''
       and source.cleansed_postal_code             != ''      
   
      -- Matching Criteria 5: First Name, Last Name, Email, Name Suffix --             
      
     LEFT OUTER JOIN (
       SELECT
         MAX(contact_adw_key) AS contact_adw_key
         , cleansed_first_name
         , cleansed_last_name
         , cleansed_suffix_name 
         , cleansed_email_name
       FROM
         `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
		 WHERE coalesce(cleansed_email_name, '') != ''
       group by 
           cleansed_first_name
         , cleansed_last_name
         , cleansed_suffix_name 
         , cleansed_email_name
         ) email_match
     ON
         source.cleansed_first_name       = email_match.cleansed_first_name
     and source.cleansed_last_name        = email_match.cleansed_last_name
     and source.cleansed_suffix_name      = email_match.cleansed_suffix_name                     
     and source.cleansed_email_name            = email_match.cleansed_email_name
     and source.cleansed_first_name      != ''
     and source.cleansed_last_name       != ''             
     and source.cleansed_email_name           != ''        
"""

contact_work_unmatched_1 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` as
with member_guid as
(select
GENERATE_UUID() as contact_adw_key,
    client_unique_entity
from (select
    distinct 
    client_unique_entity
    from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_matched`
    where contact_adw_key is null)
) 
select
b.contact_adw_key as contact_adw_key,
original.client_unique_entity,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
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
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_matched` original
left outer join member_guid b on 
original.client_unique_entity=b.client_unique_entity
where original.contact_adw_key is null
"""
contact_work_unmatched_2 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_2` as
with member_id_guid as
(
select 
GENERATE_UUID() as contact_adw_key,
club_cd,
membership_id,
associate_id
from (select distinct 
            club_cd,
            membership_id,
            associate_id
            from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1`
			where coalesce(membership_id, '') != ''
            )
),
matched_guid as
(
    select
        a.contact_adw_key as orig_contact_adw_key
        , min(b.contact_adw_key) as new_contact_adw_key
        from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` a
        join member_id_guid b on 
        a.club_cd=b.club_cd and
        a.membership_id=b.membership_id and
        a.associate_id=b.associate_id 
        group by a.contact_adw_key
)
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.client_unique_entity,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
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

from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` original
left outer join matched_guid matched on 
original.contact_adw_key = matched.orig_contact_adw_key
"""

contact_work_unmatched_3 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_3` as
with address_guid as
(
         SELECT
           DISTINCT 
           contact_adw_key,                     
           cleansed_first_name,
           cleansed_last_name,
		   cleansed_suffix_name,
           cleansed_address_1_name,
           cleansed_city_name,
           cleansed_state_code
         FROM
           `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_2`
         WHERE
               cleansed_first_name      != ''
           and cleansed_last_name       != ''
           and cleansed_address_1_name   != ''
           and cleansed_city_name            != ''
           and cleansed_state_code           != ''
             ),
       matched_guid AS (
       SELECT
         a.contact_adw_key AS orig_contact_adw_key,
         MIN(b.contact_adw_key) AS new_contact_adw_key
       FROM
         `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_2` a
       JOIN
         address_guid b
       ON
           a.cleansed_first_name       = b.cleansed_first_name
       and a.cleansed_last_name        = b.cleansed_last_name
       and a.cleansed_suffix_name      = b.cleansed_suffix_name
       and a.cleansed_address_1_name    = b.cleansed_address_1_name
       and a.cleansed_city_name             = b.cleansed_city_name    
       and a.cleansed_state_code            = b.cleansed_state_code
       GROUP BY
         a.contact_adw_key )
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.client_unique_entity,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_2` original
left outer join matched_guid matched on original.contact_adw_key=matched.orig_contact_adw_key

"""
contact_work_unmatched_4 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_4` as
with address_guid as
(
         SELECT
           DISTINCT 
           contact_adw_key,                      
           cleansed_first_name,
           cleansed_last_name,
		   cleansed_suffix_name,
           cleansed_address_1_name,
           cleansed_postal_code
         FROM
           `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_3`
         WHERE
               cleansed_first_name      != ''
           and cleansed_last_name       != ''
           and cleansed_address_1_name   != ''
           and cleansed_postal_code             != ''
             ),
       matched_guid AS (
       SELECT
         a.contact_adw_key AS orig_contact_adw_key,
         MIN(b.contact_adw_key) AS new_contact_adw_key
       FROM
         `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_3` a
       JOIN
         address_guid b
       ON
           a.cleansed_first_name       = b.cleansed_first_name
       and a.cleansed_last_name        = b.cleansed_last_name
       and a.cleansed_suffix_name      = b.cleansed_suffix_name
       and a.cleansed_address_1_name    = b.cleansed_address_1_name
       and a.cleansed_postal_code             = b.cleansed_postal_code    
       GROUP BY
         a.contact_adw_key )
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.client_unique_entity,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_3` original
left outer join matched_guid matched on original.contact_adw_key=matched.orig_contact_adw_key
"""
contact_work_unmatched_5 = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_5` as

with email_guid as
(
select 
GENERATE_UUID() as contact_adw_key ,
cleansed_first_name,
cleansed_last_name,
cleansed_email_name,
cleansed_suffix_name
from (         
        SELECT
           DISTINCT 
             cleansed_first_name,
             cleansed_last_name,
             cleansed_email_name,           
             cleansed_suffix_name
         FROM
           `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_4`
         WHERE
      	        cleansed_first_name      != ''
      	    and cleansed_last_name       != ''  
      	    and cleansed_email_name           != '')    
),

       matched_guid AS (
       SELECT
         a.contact_adw_key AS orig_contact_adw_key,
         MIN(b.contact_adw_key ) AS new_contact_adw_key
       FROM
         `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_4` a
       JOIN
         email_guid b
       ON
             a.cleansed_first_name       = b.cleansed_first_name
         and a.cleansed_last_name        = b.cleansed_last_name             
      	 and a.cleansed_email_name            = b.cleansed_email_name         
         AND a.cleansed_suffix_name      = b.cleansed_suffix_name
       GROUP BY
         a.contact_adw_key )
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.client_unique_entity,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_4` original

left outer join matched_guid matched on original.contact_adw_key = matched.orig_contact_adw_key
"""
contact_work_final_staging = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage` as 

select 
contact_adw_key ,
client_unique_entity,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_matched`
where contact_adw_key is not null

union all 

select 
contact_adw_key ,
client_unique_entity,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_5`
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
where name_adw_key not in (select distinct name_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name`)
"""
query_name_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_name_stage` as
with combined_cust_name as
(
select contact_adw_key,
name_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
union distinct
select
target.contact_adw_key,
target.name_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name` target
where contact_source_system_name='epic'
 and name_type_code like 'common%'
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
'epic' as source_system,
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
using `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_name_stage` source
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
when not matched by source and target.contact_source_system_name='epic' then delete
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
where address_adw_key not in (select distinct address_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address`)
"""
query_address_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_address_stage` as
with combined_cust_address as
(
select contact_adw_key,
address_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
union distinct
select
target.contact_adw_key,
target.address_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address` target
where contact_source_system_name='epic'
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
'epic' as source_system,
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
using `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_address_stage` source
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
when not matched by source and target.contact_source_system_name='epic' then delete
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
where phone_adw_key not in (select distinct phone_adw_key from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone`)
and raw_phone_number is not null
"""

query_phone_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_phone_stage` as
with combined_cust_phone as
(
select contact_adw_key,
phone_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
union distinct
select
target.contact_adw_key,
target.phone_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_phone` target
where contact_source_system_name='epic'
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
'epic' as source_system,
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
using `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_phone_stage` source
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
when not matched by source and target.contact_source_system_name='epic' then delete
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
where email_adw_key not in (select distinct email_adw_key  from `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email`)
and raw_email_name is not null and raw_email_name != ''
"""
query_email_bridge = f"""
create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_email_stage` as
with combined_cust_email as
(
select contact_adw_key,
email_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
union distinct
select
target.contact_adw_key,
target.email_adw_key,
target.effective_start_datetime as last_upd_dt
from `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email` target
where contact_source_system_name='epic'
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
'epic' as source_system,
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
using `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_email_stage` source
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
when not matched by source and target.contact_source_system_name='epic' then delete
"""

# KEYS
query_source_key = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_key_stage` AS
WITH
  normalized_source_key AS (
  SELECT
    contact_adw_key,
    'uniqentity_key' AS key_type_name,
    CAST(last_upd_dt AS datetime) last_upd_dt,
    client_unique_entity AS source_1_key,
    CAST(NULL AS string) AS source_2_key,
    CAST(NULL AS string) AS source_3_key,
    CAST(NULL AS string) AS source_4_key,
    CAST(NULL AS string) AS source_5_key,
    client_unique_entity AS dedupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
  UNION ALL
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
    `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
 ),
  all_keys AS (
  SELECT
    contact_adw_key,
    case 
        when key_type_name like 'uniqentity_key%' then 'uniqentity_key'
        when key_type_name like 'member_id%' then 'member_id'
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
    contact_source_system_name = 'epic'
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
	'epic' AS contact_source_system_name,
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
using `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_key_stage` source
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
when not matched by source and target.contact_source_system_name='epic' then delete
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
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_stage`
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
case when contact_source_system_name='mzp' then 'home'
     when contact_source_system_name='d3' then 'home'
	 when contact_source_system_name='epic' then 'home' end as phone_type_code,
case when contact_source_system_name='mzp' then 'home'
     when contact_source_system_name='d3' then 'home'
	 when contact_source_system_name='epic' then 'home' end as address_type_code,
case when contact_source_system_name='mzp' then 'common'
     when contact_source_system_name='d3' then 'common'
	 when contact_source_system_name='epic' then 'common' end as name_type_code,
case when contact_source_system_name='mzp' then 'personal'
     when contact_source_system_name='d3' then 'personal'
	 when contact_source_system_name='epic' then 'personal' end as email_type_code,
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
	name_bridge.name_type_code=primacy.name_type_code and
	name_bridge.effective_end_datetime=datetime('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name 
    on name_bridge.name_adw_key=name.name_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address` address_bridge 
    on customer.contact_adw_key=address_bridge.contact_adw_key and
	address_bridge.contact_source_system_name=primacy.contact_source_system_name and
	address_bridge.address_type_code=primacy.address_type_code and
	address_bridge.effective_end_datetime=datetime('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address` address 
    on address_bridge.address_adw_key=address.address_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email` email_bridge 
    on customer.contact_adw_key=email_bridge.contact_adw_key and
	email_bridge.contact_source_system_name=primacy.contact_source_system_name and
	email_bridge.email_type_code=primacy.email_type_code and
	email_bridge.effective_end_datetime=datetime('9999-12-31')

left outer join `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email` email 
    on email_bridge.email_adw_key=email.email_adw_key

left outer join `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_phone` phone_bridge 
    on customer.contact_adw_key=phone_bridge.contact_adw_key and
	phone_bridge.contact_source_system_name=primacy.contact_source_system_name and
	phone_bridge.phone_type_code=primacy.phone_type_code and
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
target.adw_row_hash=source.adw_row_hash,
target.integrate_update_datetime=current_datetime,
target.integrate_update_batch_number=1

"""
query_audit_contact_match = f"""
insert into `{int_u.INTEGRATION_PROJECT}.adw.audit_contact_match` 
(
source_system_name,
source_system_key_1,
source_system_key_2,
source_system_key_3,
contact_adw_key,
target_match_on_source_key_indicator,
target_match_on_member_id_indicator,
target_match_on_name_address_indicator,
target_match_on_name_zip_indicator,
target_match_on_name_email_indicator,
source_match_on_member_id_indicator,
source_match_on_name_address_indicator,
source_match_on_name_zip_indicator,
source_match_on_name_email_indicator,
integrate_insert_datetime,
integrate_insert_batch_number
)
select
source_system_name,
source_system_key as source_system_key_1,
cast(null as string) source_system_key_2,
cast(null as string) source_system_key_3,
contact_adw_key,
max(case when match_system='target' and match_type='source_key_match' then '1' else '0' end) as target_match_on_source_key_indicator,
max(case when match_system='target' and match_type='source_key_match' then '1' else '0' end) as target_match_on_member_id_indicator,
max(case when match_system='target' and match_type='name_address_city_match' then '1' else '0' end) as target_match_on_name_address_indicator,
max(case when match_system='target' and match_type='name_address_zip_match' then '1' else '0' end) as target_match_on_name_zip_indicator,
max(case when match_system='target' and match_type='name_email_match' then '1' else '0' end) as target_match_on_name_email_indicator,
max(case when match_system='source' and match_type='source_key_match' then '1' else '0' end) as source_match_on_member_id_indicator,
max(case when match_system='source' and match_type='name_address_city_match' then '1' else '0' end) as source_match_on_name_address_indicator,
max(case when match_system='source' and match_type='name_address_zip_match' then '1' else '0' end) as source_match_on_name_zip_indicator,
max(case when match_system='source' and match_type='name_email_match' then '1' else '0' end) as source_match_on_name_email_indicator,
current_datetime as integrate_insert_datetime,
1 as integrate_insert_batch_number
from
(select
'epic' as source_system_name,
'target' as match_system,
'source_key_match' as match_type,
source.client_unique_entity as source_system_key,
client_key_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_work` source      
      -- Matching Criteria 1: Member Key --
join (select max(contact_adw_key) as contact_adw_key
      , client_unique_entity
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
      group by client_unique_entity
      ) client_key_match
      on source.client_unique_entity = client_key_match.client_unique_entity
union all
select
'epic' as source_system_name,
'target' as match_system,
'member_id_match' as match_type,
source.client_unique_entity as source_system_key,
member_id_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_work` source  
      -- Matching Criteria 2: Member ID (club_cd, membership_id, associate_id) and last_upd_dt between effective_dates --     
join (select max(contact_adw_key) as contact_adw_key
                      , club_cd
                      , membership_id
                      , associate_id
                      , effective_start_datetime
                      , effective_end_datetime
                      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
					  where coalesce(membership_id, '') != ''
                      group by
                      club_cd
                      , membership_id
                      , associate_id
                      , effective_start_datetime
                      , effective_end_datetime
                      ) member_id_match

    on coalesce(source.club_cd, '')    = coalesce(member_id_match.club_cd, '') and 
	coalesce(source.membership_id, '') = coalesce(member_id_match.membership_id, '') and 
	coalesce(source.associate_id, '')  = coalesce(member_id_match.associate_id, '')
union all
select
'epic' as source_system_name,
'target' as match_system,
'name_address_city_match' as match_type,
source.client_unique_entity as source_system_key,
address_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_work` source                             
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name                      
      , cleansed_address_1_name
      , cleansed_city_name
      , cleansed_state_code
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
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
union all
select
'epic' as source_system_name,
'target' as match_system,
'name_address_zip_match' as match_type,
source.client_unique_entity as source_system_key,
zip5_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_work` source       
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name                      
      , cleansed_address_1_name
      , cleansed_postal_code
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
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
union all
 select
'epic' as source_system_name,
'target' as match_system,
'name_email_match' as match_type,
source.client_unique_entity as source_system_key,
email_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_source_work` source      
      -- Matching Criteria 4: First Name, Last Name, Email, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name 
      , cleansed_email_name
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_target_work`
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
union all
 select
'epic' as source_system_name,
'source' as match_system,
'member_id_match' as match_type,
source.client_unique_entity as source_system_key,
member_id_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` source    
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix 
join (select max(cs1.contact_adw_key) as contact_adw_key
      , club_cd
      , membership_id
      , associate_id
      , effective_start_datetime
      , effective_end_datetime
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` mk1
      join `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` cs1 
      on mk1.client_unique_entity=cs1.source_1_key and 
      cs1.contact_source_system_name='epic' and
      cs1.key_type_name='uniqentity_key'
      group by
      club_cd
      , membership_id
      , associate_id
      , effective_start_datetime
      , effective_end_datetime
      ) member_id_match

    on coalesce(source.club_cd, '')    = coalesce(member_id_match.club_cd, '') and 
	coalesce(source.membership_id, '') = coalesce(member_id_match.membership_id, '') and 
	coalesce(source.associate_id, '')  = coalesce(member_id_match.associate_id, '') and 
	coalesce(source.membership_id, '') != ''
union all
 select
'epic' as source_system_name,
'source' as match_system,
'name_address_city_match' as match_type,
source.client_unique_entity as source_system_key,
address_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` source    
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name                      
      , cleansed_address_1_name
      , cleansed_city_name
      , cleansed_state_code
	  , mk1.client_unique_entity
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` mk1
      join `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` cs1 
      on mk1.client_unique_entity=cs1.source_1_key and 
      cs1.contact_source_system_name='epic' and
      cs1.key_type_name='uniqentity_key'
      group by 
      cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name                      
      , cleansed_address_1_name
      , cleansed_city_name
      , cleansed_state_code
	  , mk1.client_unique_entity
      ) address_match
      on source.cleansed_first_name      = address_match.cleansed_first_name    
      and source.cleansed_last_name       = address_match.cleansed_last_name     
      and source.cleansed_suffix_name     = address_match.cleansed_suffix_name     
      and source.cleansed_address_1_name  = address_match.cleansed_address_1_name  
      and source.cleansed_city_name       = address_match.cleansed_city_name         
      and source.cleansed_state_code      = address_match.cleansed_state_code
	  and source.client_unique_entity     != address_match.client_unique_entity	  
      and source.cleansed_first_name      != ''                          
      and source.cleansed_last_name       != ''                                    
      and source.cleansed_address_1_name  != ''                           
      and source.cleansed_city_name       != ''                          
      and source.cleansed_state_code      != ''
union all
select
'epic' as source_system_name,
'target' as match_system,
'name_address_zip_match' as match_type,
source.client_unique_entity as source_system_key,
zip5_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` source       
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name                      
      , cleansed_address_1_name
      , cleansed_postal_code
	  , mk1.client_unique_entity
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` mk1
      join `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` cs1 
      on mk1.client_unique_entity=cs1.source_1_key and 
      cs1.contact_source_system_name='epic' and
      cs1.key_type_name='uniqentity_key'
      group by 
      cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name                      
      , cleansed_address_1_name
      , cleansed_postal_code
	  , mk1.client_unique_entity
      ) zip5_match
      on  source.cleansed_first_name      = zip5_match.cleansed_first_name
      and source.cleansed_last_name        = zip5_match.cleansed_last_name
      and source.cleansed_suffix_name      = zip5_match.cleansed_suffix_name                     
      and source.cleansed_address_1_name   = zip5_match.cleansed_address_1_name
      and source.cleansed_postal_code      = zip5_match.cleansed_postal_code
	  and source.client_unique_entity     != zip5_match.client_unique_entity
      and source.cleansed_first_name      != ''
      and source.cleansed_last_name       != ''                 
      and source.cleansed_address_1_name  != ''
      and source.cleansed_postal_code     != ''
union all
 select
'epic' as source_system_name,
'target' as match_system,
'name_email_match' as match_type,
source.client_unique_entity as source_system_key,
email_match.contact_adw_key as contact_adw_key
from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` source      
      -- Matching Criteria 4: First Name, Last Name, Email, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name 
      , cleansed_email_name
	  , mk1.client_unique_entity
      from `{int_u.INTEGRATION_PROJECT}.adw_work.epic_contact_unmatched_work_1` mk1
      join `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` cs1 
      on mk1.client_unique_entity=cs1.source_1_key and 
      cs1.contact_source_system_name='epic' and
      cs1.key_type_name='uniqentity_key'
      group by 
      cleansed_first_name
      , cleansed_last_name
      , cleansed_suffix_name 
      , cleansed_email_name
	  , mk1.client_unique_entity
      ) email_match
      on  source.cleansed_first_name       = email_match.cleansed_first_name
      and source.cleansed_last_name        = email_match.cleansed_last_name
      and source.cleansed_suffix_name      = email_match.cleansed_suffix_name                     
      and source.cleansed_email_name       = email_match.cleansed_email_name
	  and source.client_unique_entity     != email_match.client_unique_entity
      and source.cleansed_first_name      != ''
      and source.cleansed_last_name       != ''             
      and source.cleansed_email_name      != ''
) a
group by source_system_name,
source_system_key,
contact_adw_key
"""

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    # Matching Logic
    task_contact_work_source = int_u.run_query("build_contact_work_source", contact_work_source)
    task_contact_work_target = int_u.run_query("build_contact_work_target", contact_work_target)
    task_contact_work_matched = int_u.run_query("build_contact_work_matched", contact_work_matched)
    task_contact_work_unmatched_1 = int_u.run_query("build_contact_work_unmatched_1", contact_work_unmatched_1)
    task_contact_work_unmatched_2 = int_u.run_query("build_contact_work_unmatched_2", contact_work_unmatched_2)
    task_contact_work_unmatched_3 = int_u.run_query("build_contact_work_unmatched_3", contact_work_unmatched_3)
    task_contact_work_unmatched_4 = int_u.run_query("build_contact_work_unmatched_4", contact_work_unmatched_4)
    task_contact_work_unmatched_5 = int_u.run_query("build_contact_work_unmatched_5", contact_work_unmatched_5)
    task_contact_work_final_staging = int_u.run_query("build_contact_work_final_staging", contact_work_final_staging)

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
    task_contact_insert = int_u.run_query('insert_contact', query_contact_insert)
    task_contact_merge = int_u.run_query('merge_contact', query_contact_merge)
    task_contact_audit = int_u.run_query('insert_audit_contact_match', query_audit_contact_match)

    ######################################################################
    # DEPENDENCIES

    task_contact_work_source >> task_contact_work_matched
    task_contact_work_target >> task_contact_work_matched

    task_contact_work_matched >> \
    task_contact_work_unmatched_1 >> \
    task_contact_work_unmatched_2 >> \
    task_contact_work_unmatched_3 >> \
    task_contact_work_unmatched_4 >> \
    task_contact_work_unmatched_5 >> \
    task_contact_work_final_staging

    task_contact_work_final_staging >> task_name_insert >> task_contact_insert
    task_contact_work_final_staging >> task_name_bridge >> task_name_merge >> task_contact_insert

    task_contact_work_final_staging >> task_address_insert >> task_contact_insert
    task_contact_work_final_staging >> task_address_bridge >> task_address_merge >> task_contact_insert

    task_contact_work_final_staging >> task_phone_insert >> task_contact_insert
    task_contact_work_final_staging >> task_phone_bridge >> task_phone_merge >> task_contact_insert

    task_contact_work_final_staging >> task_email_insert >> task_contact_insert
    task_contact_work_final_staging >> task_email_bridge >> task_email_merge >> task_contact_insert

    task_contact_work_final_staging >> task_keys >> task_keys_bridge >> task_contact_insert >> task_contact_merge >> task_contact_audit

