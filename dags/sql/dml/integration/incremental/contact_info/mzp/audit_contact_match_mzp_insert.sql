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
coalesce(source_system_key,'-1') as source_system_key_1,
'-1' as  source_system_key_2,
'-1' as  source_system_key_3,
contact_adw_key,
max(case when match_system='target' and match_type='source_key_match' then 'Y' else 'N' end) as tgt_mch_on_source_key_ind,
'N' as tgt_mch_on_mbr_id_ind,
max(case when match_system='target' and match_type='name_address_city_match' then 'Y' else 'N' end) as tgt_mch_on_address_nm_ind,
max(case when match_system='target' and match_type='name_address_zip_match' then 'Y' else 'N' end) as tgt_mch_on_zip_nm_ind,
max(case when match_system='target' and match_type='name_email_match' then 'Y' else 'N' end) as tgt_mch_on_email_nm_ind,
'N' as src_mch_on_mbr_id_ind,
max(case when match_system='source' and match_type='name_address_city_match' then 'Y' else 'N' end) as src_mch_on_address_nm_ind,
max(case when match_system='source' and match_type='name_address_zip_match' then 'Y' else 'N' end) as src_mch_on_zip_nm_ind,
max(case when match_system='source' and match_type='name_email_match' then 'Y' else 'N' end) as src_mch_on_email_nm_ind,
current_datetime as integrate_insert_datetime,
{{ dag_run.id }} as integrate_insert_batch_number
from
(select
'mzp' as source_system_nm,
'target' as match_system,
'source_key_match' as match_type,
source.member_ky as source_system_key,
member_key_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_source_work` source
      -- Matching Criteria 1: Derived Member Key --
join (select max(contact_adw_key) as contact_adw_key
      , derived_member_ky
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
      group by derived_member_ky
      ) member_key_match
      on source.derived_member_ky = member_key_match.derived_member_ky
union all
select
'mzp' as source_system_nm,
'target' as match_system,
'name_address_city_match' as match_type,
source.member_ky as source_system_key,
address_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_source_work` source
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_address_1_nm
      , cleansed_city_nm
      , cleansed_state_cd
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
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
'mzp' as source_system_nm,
'target' as match_system,
'name_address_zip_match' as match_type,
source.member_ky as source_system_key,
zip5_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_source_work` source
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_address_1_nm
      , cleansed_postal_cd
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
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
'mzp' as source_system_nm,
'target' as match_system,
'name_email_match' as match_type,
source.member_ky as source_system_key,
email_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_source_work` source
      -- Matching Criteria 4: First Name, Last Name, Email, Name Suffix --
join (select max(contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_email_nm
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
      group by
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_email_nm
      ) email_match
      on  source.cleansed_first_nm       = email_match.cleansed_first_nm
      and source.cleansed_last_nm        = email_match.cleansed_last_nm
      and source.cleansed_suffix_nm      = email_match.cleansed_suffix_nm
      and source.cleansed_email_nm       = email_match.cleansed_email_nm
      and source.cleansed_first_nm      != ''
      and source.cleansed_last_nm       != ''
      and source.cleansed_email_nm      != ''
union all
 select
'mzp' as source_system_nm,
'source' as match_system,
'name_address_city_match' as match_type,
source.member_ky as source_system_key,
address_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` source
      -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_address_1_nm
      , cleansed_city_nm
      , cleansed_state_cd
	  , mk1.derived_member_ky
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` mk1
      join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` cs1
      on mk1.derived_member_ky=cs1.source_1_key and
      cs1.contact_source_system_nm='mzp' and
      cs1.key_typ_nm='derived_member_key'
      group by
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_address_1_nm
      , cleansed_city_nm
      , cleansed_state_cd
	  , mk1.derived_member_ky
      ) address_match
      on source.cleansed_first_nm      = address_match.cleansed_first_nm
      and source.cleansed_last_nm       = address_match.cleansed_last_nm
      and source.cleansed_suffix_nm     = address_match.cleansed_suffix_nm
      and source.cleansed_address_1_nm  = address_match.cleansed_address_1_nm
      and source.cleansed_city_nm       = address_match.cleansed_city_nm
      and source.cleansed_state_cd      = address_match.cleansed_state_cd
	    and source.derived_member_ky                != address_match.derived_member_ky
      and source.cleansed_first_nm      != ''
      and source.cleansed_last_nm       != ''
      and source.cleansed_address_1_nm  != ''
      and source.cleansed_city_nm       != ''
      and source.cleansed_state_cd      != ''
union all
select
'mzp' as source_system_nm,
'source' as match_system,
'name_address_zip_match' as match_type,
source.member_ky as source_system_key,
zip5_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` source
      -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_address_1_nm
      , cleansed_postal_cd
	  , mk1.derived_member_ky
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` mk1
      join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` cs1
      on mk1.derived_member_ky=cs1.source_1_key and
      cs1.contact_source_system_nm='mzp' and
      cs1.key_typ_nm='derived_member_key'
      group by
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_address_1_nm
      , cleansed_postal_cd
	  , mk1.derived_member_ky
      ) zip5_match
      on  source.cleansed_first_nm      = zip5_match.cleansed_first_nm
      and source.cleansed_last_nm        = zip5_match.cleansed_last_nm
      and source.cleansed_suffix_nm      = zip5_match.cleansed_suffix_nm
      and source.cleansed_address_1_nm   = zip5_match.cleansed_address_1_nm
      and source.cleansed_postal_cd      = zip5_match.cleansed_postal_cd
	    and source.derived_member_ky                != zip5_match.derived_member_ky
      and source.cleansed_first_nm      != ''
      and source.cleansed_last_nm       != ''
      and source.cleansed_address_1_nm  != ''
      and source.cleansed_postal_cd     != ''
union all
 select
'mzp' as source_system_nm,
'source' as match_system,
'name_email_match' as match_type,
source.member_ky as source_system_key,
email_match.contact_adw_key as contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` source
      -- Matching Criteria 4: First Name, Last Name, Email, Name Suffix --
join (select max(cs1.contact_adw_key) as contact_adw_key
      , cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_email_nm
	  , mk1.derived_member_ky
      from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` mk1
      join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` cs1
      on mk1.derived_member_ky=cs1.source_1_key and
      cs1.contact_source_system_nm='mzp' and
      cs1.key_typ_nm='derived_member_key'
      group by
      cleansed_first_nm
      , cleansed_last_nm
      , cleansed_suffix_nm
      , cleansed_email_nm
	  , mk1.derived_member_ky
      ) email_match
      on  source.cleansed_first_nm       = email_match.cleansed_first_nm
      and source.cleansed_last_nm        = email_match.cleansed_last_nm
      and source.cleansed_suffix_nm      = email_match.cleansed_suffix_nm
      and source.cleansed_email_nm       = email_match.cleansed_email_nm
	  and source.derived_member_ky                != email_match.derived_member_ky
      and source.cleansed_first_nm      != ''
      and source.cleansed_last_nm       != ''
      and source.cleansed_email_nm      != ''
) a
group by source_system_nm,
source_system_key,
contact_adw_key