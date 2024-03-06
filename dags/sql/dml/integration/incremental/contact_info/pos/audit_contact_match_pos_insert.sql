insert into `{{ var.value.INTEGRATION_PROJECT }}.adw.audit_contact_match`
(
source_system_nm,
source_system_key_1,
source_system_key_2,
source_system_key_3,
contact_adw_key,
tgt_mch_on_source_key_ind,
tgt_mch_on_mbr_id_ind,
tgt_mch_on_address_nm_ind,
tgt_mch_on_zip_nm_ind,
tgt_mch_on_email_nm_ind,
src_mch_on_mbr_id_ind,
src_mch_on_address_nm_ind,
src_mch_on_zip_nm_ind,
src_mch_on_email_nm_ind,
integrate_insert_datetime,
integrate_insert_batch_number
)
select
source_system_nm,
coalesce(source_system_key_1,'-1') as source_system_key_1,
coalesce(source_system_key_2,'-1') source_system_key_2,
coalesce(source_system_key_3,'-1') source_system_key_3,
contact_adw_key,
max(case when match_system='target' and match_type='source_key_match' then 'Y' else 'N' end) as tgt_mch_on_source_key_ind,
max(case when match_system='target' and match_type='member_id_match' then 'Y' else 'N' end) as tgt_mch_on_mbr_id_ind,
max(case when match_system='target' and match_type='name_address_city_match' then 'Y' else 'N' end) as tgt_mch_on_address_nm_ind,
max(case when match_system='target' and match_type='name_address_zip_match' then 'Y' else 'N' end) as tgt_mch_on_zip_nm_ind,
'N' as tgt_mch_on_email_nm_ind,
max(case when match_system='source' and match_type='member_id_match' then 'Y' else 'N' end) as src_mch_on_mbr_id_ind,
max(case when match_system='source' and match_type='name_address_city_match' then 'Y' else 'N' end) as src_mch_on_address_nm_ind,
max(case when match_system='source' and match_type='name_address_zip_match' then 'Y' else 'N' end) as src_mch_on_zip_nm_ind,
'N' as src_mch_on_email_nm_ind,
current_datetime as integrate_insert_datetime,
{{ dag_run.id }} as integrate_insert_batch_number
from
(select
'pos' as source_system_nm,
'target' as match_system,
'source_key_match' as match_type,
source.pos_cust_id as source_system_key_1,
'-1' as source_system_key_2,
'-1' as source_system_key_3,
pos_key_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_source_work` source      
      -- Matching Criteria 1: D3 Key --
join (select max(contact_adw_key) as contact_adw_key
      , pos_cust_id
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_target_work`
      group by pos_cust_id
      ) pos_key_match
      on source.pos_cust_id = pos_key_match.pos_cust_id
union all
select
'pos' as source_system_nm,
'target' as match_system,
'member_id_match' as match_type,
source.pos_cust_id as source_system_key_1,
'-1' as source_system_key_2,
'-1' as source_system_key_3,
member_id_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_source_work` source  
      -- Matching Criteria 2: Member ID (club_cd, membership_id, associate_id) and last_upd_dt between effective_dates --     
join (select max(contact_adw_key) as contact_adw_key
                      , club_cd
                      , membership_id
                      , associate_id
                      , effective_start_datetime
                      , effective_end_datetime
                      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_target_work`
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
'pos' as source_system_nm,
'target' as match_system,
'name_address_city_match' as match_type,
source.pos_cust_id as source_system_key_1,
'-1' as source_system_key_2,
'-1' as source_system_key_3,
address_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_source_work` source                             
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_city_nm
      , cleansed_state_cd
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_target_work`
      group by 
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_city_nm
      , cleansed_state_cd
      ) address_match
      on source.cleansed_first_nm      = address_match.cleansed_first_nm    
      and source.cleansed_last_nm       = address_match.cleansed_last_nm     
      and source.cleansed_suffix_nm     = address_match.cleansed_suffix_nm     
      and source.cleansed_address_1_nm  = address_match.cleansed_address_1_nm  
      and source.cleansed_city_nm       = address_match.cleansed_city_nm         
      and source.cleansed_state_cd      = address_match.cleansed_state_cd        
      and source.cleansed_first_nm      != ''                          
      and source.cleansed_last_nm       != ''                                    
      and source.cleansed_address_1_nm  != ''                           
      and source.cleansed_city_nm       != ''                          
      and source.cleansed_state_cd      != ''                          
union all
select
'pos' as source_system_nm,
'target' as match_system,
'name_address_zip_match' as match_type,
source.pos_cust_id as source_system_key_1,
'-1' as source_system_key_2,
'-1' as source_system_key_3,
zip5_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_source_work` source       
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_postal_cd
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_target_work`
      group by 
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_postal_cd
      ) zip5_match
      on  source.cleansed_first_nm      = zip5_match.cleansed_first_nm
      and source.cleansed_last_nm        = zip5_match.cleansed_last_nm
      and source.cleansed_suffix_nm      = zip5_match.cleansed_suffix_nm                     
      and source.cleansed_address_1_nm   = zip5_match.cleansed_address_1_nm
      and source.cleansed_postal_cd      = zip5_match.cleansed_postal_cd
      and source.cleansed_first_nm      != ''
      and source.cleansed_last_nm       != ''                 
      and source.cleansed_address_1_nm  != ''
      and source.cleansed_postal_cd     != ''
union all
 select
'pos' as source_system_nm,
'source' as match_system,
'member_id_match' as match_type,
source.pos_cust_id as source_system_key_1,
'-1' as source_system_key_2,
'-1' as source_system_key_3,
member_id_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_unmatched_work_1` source    
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix 
join (select max(cs1.contact_adw_key) as contact_adw_key
      , club_cd
      , membership_id
      , associate_id
      , effective_start_datetime
      , effective_end_datetime
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_unmatched_work_1` mk1
      join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` cs1 
      on mk1.pos_cust_id=cs1.source_1_key and
      cs1.contact_source_system_nm='pos' and
      cs1.key_typ_nm='pos_cust_id'
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
'pos' as source_system_nm,
'source' as match_system,
'name_address_city_match' as match_type,
source.pos_cust_id as source_system_key_1,
'-1' as source_system_key_2,
'-1' as source_system_key_3,
address_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_unmatched_work_1` source    
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_city_nm
      , cleansed_state_cd
        , mk1.pos_cust_id
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_unmatched_work_1` mk1
      join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` cs1 
      on mk1.pos_cust_id=cs1.source_1_key and
      cs1.contact_source_system_nm='pos' and
      cs1.key_typ_nm='pos_cust_id'
      group by 
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_city_nm
      , cleansed_state_cd
        , mk1.pos_cust_id
      ) address_match
      on source.cleansed_first_nm      = address_match.cleansed_first_nm    
      and source.cleansed_last_nm       = address_match.cleansed_last_nm     
      and source.cleansed_suffix_nm     = address_match.cleansed_suffix_nm     
      and source.cleansed_address_1_nm  = address_match.cleansed_address_1_nm  
      and source.cleansed_city_nm       = address_match.cleansed_city_nm         
      and source.cleansed_state_cd      = address_match.cleansed_state_cd
      and source.pos_cust_id             != address_match.pos_cust_id
      and source.cleansed_first_nm      != ''                          
      and source.cleansed_last_nm       != ''                                    
      and source.cleansed_address_1_nm  != ''                           
      and source.cleansed_city_nm       != ''                          
      and source.cleansed_state_cd      != ''
union all
select
'pos' as source_system_nm,
'source' as match_system,
'name_address_zip_match' as match_type,
source.pos_cust_id as source_system_key_1,
'-1' as source_system_key_2,
'-1' as source_system_key_3,
zip5_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_unmatched_work_1` source       
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_postal_cd
        , mk1.pos_cust_id
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.pos_contact_unmatched_work_1` mk1
      join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` cs1 
      on mk1.pos_cust_id=cs1.source_1_key and
      cs1.contact_source_system_nm='pos' and
      cs1.key_typ_nm='pos_cust_id'
      group by 
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm                      
      , cleansed_address_1_nm
      , cleansed_postal_cd
        , mk1.pos_cust_id
      ) zip5_match
      on  source.cleansed_first_nm      = zip5_match.cleansed_first_nm
      and source.cleansed_last_nm        = zip5_match.cleansed_last_nm
      and source.cleansed_suffix_nm      = zip5_match.cleansed_suffix_nm                     
      and source.cleansed_address_1_nm   = zip5_match.cleansed_address_1_nm
      and source.cleansed_postal_cd      = zip5_match.cleansed_postal_cd
      and source.pos_cust_id             != zip5_match.pos_cust_id
      and source.cleansed_first_nm      != ''
      and source.cleansed_last_nm       != ''                 
      and source.cleansed_address_1_nm  != ''
      and source.cleansed_postal_cd     != ''
) a
group by source_system_nm,
source_system_key_1,
source_system_key_2,
source_system_key_3,
contact_adw_key
