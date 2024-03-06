--initial matching to the target
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_matched` as
select
coalesce(member_id_match.contact_adw_key,
        address_match.contact_adw_key,
        zip5_match.contact_adw_key,
        service_call_match.contact_adw_key) as contact_adw_key,

source.iso_cd,
source.club_cd,
source.membership_id,
source.associate_id,
source.check_digit_nr,
source.comm_ctr_id,
source.sc_id,
source.sc_dt,
source.address_adw_key,
source.nm_adw_key,
source.phone_adw_key,
source.gender,
source.birthdate,
source.last_upd_dt,
source.cleansed_first_nm,
source.cleansed_last_nm,
source.cleansed_suffix_nm,
source.cleansed_address_1_nm,
source.cleansed_city_nm,
source.cleansed_state_cd,
source.cleansed_postal_cd

from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_source_work` source


      -- Matching Criteria 1: Member ID (club_cd, membership_id, associate_id) and last_upd_dt between effective_dates --     
left outer join (select max(contact_adw_key) as contact_adw_key
                      , club_cd
                      , membership_id
                      , associate_id
                      , effective_start_datetime
                      , effective_end_datetime
                      , mbr_id_count
                      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_target_work`
                      where coalesce(membership_id,'') != '' and membership_id != '0000000'
                      group by
                      club_cd
                      , membership_id
                      , associate_id
                      , effective_start_datetime
                      , effective_end_datetime
                      , mbr_id_count
                      ) member_id_match

    on source.club_cd    = member_id_match.club_cd and 
    source.membership_id = member_id_match.membership_id and 
    source.associate_id  = member_id_match.associate_id and 
    (
    (cast(last_upd_dt as datetime) between effective_start_datetime and effective_end_datetime) or (mbr_id_count = 1)
    )

      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --            
     LEFT OUTER JOIN (
       SELECT
         MAX(contact_adw_key) AS contact_adw_key
         , cleansed_first_nm
         , cleansed_last_nm
         , cleansed_suffix_nm                      
         , cleansed_address_1_nm
         , cleansed_city_nm
         , cleansed_state_cd
       FROM
         `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_target_work`
       WHERE cleansed_city_nm != ''
       GROUP BY
           cleansed_first_nm
         , cleansed_last_nm
         , cleansed_suffix_nm                      
         , cleansed_address_1_nm
         , cleansed_city_nm
         , cleansed_state_cd ) address_match
       ON
            source.cleansed_first_nm       = address_match.cleansed_first_nm
        and source.cleansed_last_nm        = address_match.cleansed_last_nm
        and source.cleansed_suffix_nm      = address_match.cleansed_suffix_nm                     
        and source.cleansed_address_1_nm    = address_match.cleansed_address_1_nm
        and source.cleansed_city_nm             = address_match.cleansed_city_nm
        and source.cleansed_state_cd            = address_match.cleansed_state_cd    
        and source.cleansed_first_nm      != ''
        and source.cleansed_last_nm       != ''                  
        and source.cleansed_address_1_nm   != ''
        and source.cleansed_city_nm            != ''
        and source.cleansed_state_cd           != ''        
    
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --        

     LEFT OUTER JOIN (
       SELECT
         MAX(contact_adw_key) AS contact_adw_key
         , cleansed_first_nm
         , cleansed_last_nm
         , cleansed_suffix_nm                      
         , cleansed_address_1_nm
         , cleansed_postal_cd
       FROM
         `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_target_work`
       WHERE cleansed_postal_cd != ''
       GROUP BY
           cleansed_first_nm
         , cleansed_last_nm
         , cleansed_suffix_nm                      
         , cleansed_address_1_nm
         , cleansed_postal_cd ) zip5_match
       ON
           source.cleansed_first_nm       = zip5_match.cleansed_first_nm
       and source.cleansed_last_nm        = zip5_match.cleansed_last_nm
       and source.cleansed_suffix_nm      = zip5_match.cleansed_suffix_nm                     
       and source.cleansed_address_1_nm    = zip5_match.cleansed_address_1_nm
       and source.cleansed_postal_cd              = zip5_match.cleansed_postal_cd
       and source.cleansed_first_nm      != ''
       and source.cleansed_last_nm       != ''                  
       and source.cleansed_address_1_nm   != ''
       and source.cleansed_postal_cd             != ''      
   
      -- Matching Criteria 4: Service Call Key --       
left outer join (select max(contact_adw_key) as contact_adw_key
                      , comm_ctr_id
                      , sc_id
                      , sc_dt
                      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_target_work`
                      where coalesce(sc_id,'') != ''
                      group by comm_ctr_id, sc_id, sc_dt
                      ) service_call_match 
                      on source.comm_ctr_id=service_call_match.comm_ctr_id 
                      and source.sc_id=service_call_match.sc_id
                      and source.sc_dt=service_call_match.sc_dt
;
--self matching on source key
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_1` as
with member_guid as
(select
GENERATE_UUID() as contact_adw_key
  , comm_ctr_id
  , sc_id
  , sc_dt
from (select
    distinct 
    comm_ctr_id
  , sc_id
  , sc_dt
    from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_matched`
    where contact_adw_key is null)
) 
select
b.contact_adw_key as contact_adw_key,
original.comm_ctr_id,
original.sc_id,
original.sc_dt,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.address_adw_key,
original.nm_adw_key,
original.phone_adw_key,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_nm,
original.cleansed_last_nm,
original.cleansed_suffix_nm,
original.cleansed_address_1_nm,
original.cleansed_city_nm,
original.cleansed_state_cd,
original.cleansed_postal_cd  
FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_matched` original
left outer join member_guid b on 
original.comm_ctr_id=b.comm_ctr_id and
original.sc_id=b.sc_id and
original.sc_dt=b.sc_dt
where original.contact_adw_key is null;
--self matching on membership_id
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_2` as
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
            from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_1`
            where membership_id != '0000000'
            )
),
matched_guid as
(
    select
        a.contact_adw_key as orig_contact_adw_key
        , min(b.contact_adw_key) as new_contact_adw_key
        from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_1` a
        join member_id_guid b on 
        a.club_cd=b.club_cd and
        a.membership_id=b.membership_id and
        a.associate_id=b.associate_id
        group by a.contact_adw_key
)
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.comm_ctr_id,
original.sc_id,
original.sc_dt,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.address_adw_key,
original.nm_adw_key,
original.phone_adw_key,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_nm,
original.cleansed_last_nm,
original.cleansed_suffix_nm,
original.cleansed_address_1_nm,
original.cleansed_city_nm,
original.cleansed_state_cd,
original.cleansed_postal_cd  

from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_1` original
left outer join matched_guid matched on 
original.contact_adw_key = matched.orig_contact_adw_key
;
--self matching on name and address_match
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_3` as
with address_guid as
(
         SELECT
           DISTINCT 
           contact_adw_key,                     
           cleansed_first_nm,
           cleansed_last_nm,
           cleansed_suffix_nm,
           cleansed_address_1_nm,
           cleansed_city_nm,
           cleansed_state_cd
         FROM
           `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_2`
         WHERE
               cleansed_first_nm      != ''
           and cleansed_last_nm       != ''
           and cleansed_address_1_nm   != ''
           and cleansed_city_nm            != ''
           and cleansed_state_cd           != ''
             ),
       matched_guid AS (
       SELECT
         a.contact_adw_key AS orig_contact_adw_key,
         MIN(b.contact_adw_key) AS new_contact_adw_key
       FROM
         `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_2` a
       JOIN
         address_guid b
       ON
           a.cleansed_first_nm       = b.cleansed_first_nm
       and a.cleansed_last_nm        = b.cleansed_last_nm
       and a.cleansed_suffix_nm      = b.cleansed_suffix_nm
       and a.cleansed_address_1_nm    = b.cleansed_address_1_nm
       and a.cleansed_city_nm             = b.cleansed_city_nm    
       and a.cleansed_state_cd            = b.cleansed_state_cd
       GROUP BY
         a.contact_adw_key )
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.comm_ctr_id,
original.sc_id,
original.sc_dt,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.address_adw_key,
original.nm_adw_key,
original.phone_adw_key,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_nm,
original.cleansed_last_nm,
original.cleansed_suffix_nm,
original.cleansed_address_1_nm,
original.cleansed_city_nm,
original.cleansed_state_cd,
original.cleansed_postal_cd   
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_2` original
left outer join matched_guid matched on original.contact_adw_key=matched.orig_contact_adw_key
;
--self matching on name address with zip
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_4` as
with address_guid as
(
         SELECT
           DISTINCT 
           contact_adw_key,                      
           cleansed_first_nm,
           cleansed_last_nm,
           cleansed_suffix_nm,
           cleansed_address_1_nm,
           cleansed_postal_cd
         FROM
           `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_3`
         WHERE
               cleansed_first_nm      != ''
           and cleansed_last_nm       != ''
           and cleansed_address_1_nm   != ''
           and cleansed_postal_cd             != ''
             ),
       matched_guid AS (
       SELECT
         a.contact_adw_key AS orig_contact_adw_key,
         MIN(b.contact_adw_key) AS new_contact_adw_key
       FROM
         `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_3` a
       JOIN
         address_guid b
       ON
           a.cleansed_first_nm       = b.cleansed_first_nm
       and a.cleansed_last_nm        = b.cleansed_last_nm
       and a.cleansed_suffix_nm      = b.cleansed_suffix_nm
       and a.cleansed_address_1_nm    = b.cleansed_address_1_nm
       and a.cleansed_postal_cd             = b.cleansed_postal_cd    
       GROUP BY
         a.contact_adw_key )
select 
coalesce(matched.new_contact_adw_key,contact_adw_key) as contact_adw_key,
original.comm_ctr_id,
original.sc_id,
original.sc_dt,
original.iso_cd,
original.club_cd,
original.membership_id,
original.associate_id,
original.check_digit_nr,
original.address_adw_key,
original.nm_adw_key,
original.phone_adw_key,
original.gender,
original.birthdate,
original.last_upd_dt,
original.cleansed_first_nm,
original.cleansed_last_nm,
original.cleansed_suffix_nm,
original.cleansed_address_1_nm,
original.cleansed_city_nm,
original.cleansed_state_cd,
original.cleansed_postal_cd   
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_3` original
left outer join matched_guid matched on original.contact_adw_key=matched.orig_contact_adw_key
;
--final staging
create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_stage` as 
select 
contact_adw_key ,
comm_ctr_id,
sc_id,
sc_dt,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
address_adw_key,
nm_adw_key,
phone_adw_key,
gender,
birthdate,
last_upd_dt,
cleansed_first_nm,
cleansed_last_nm,
cleansed_suffix_nm,
cleansed_address_1_nm,
cleansed_city_nm,
cleansed_state_cd,
cleansed_postal_cd 
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_matched`
where contact_adw_key is not null

union all 

select 
contact_adw_key ,
comm_ctr_id,
sc_id,
sc_dt,
iso_cd,
club_cd,
membership_id,
associate_id,
check_digit_nr,
address_adw_key,
nm_adw_key,
phone_adw_key,
gender,
birthdate,
last_upd_dt,
cleansed_first_nm,
cleansed_last_nm,
cleansed_suffix_nm,
cleansed_address_1_nm,
cleansed_city_nm,
cleansed_state_cd,
cleansed_postal_cd
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_unmatched_work_4`