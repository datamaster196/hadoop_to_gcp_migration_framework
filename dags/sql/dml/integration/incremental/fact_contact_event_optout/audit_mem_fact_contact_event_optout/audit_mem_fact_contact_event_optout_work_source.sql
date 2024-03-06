CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.audit_mem_fact_contact_event_optout_work_source` AS
  ------------------------------------------------------------------------
  --------------------------------Source Member------------------------------

SELECT
  contact_adw_key,
  source_system_key,
  system_of_record,
  topic_nm,
  channel,
  event_cd,
  event_dtm,
  dupe_check
  FROM (
		SELECT contact_adw_key, source_system_key, system_of_record, topic_nm, channel, event_cd, event_dtm,
		  ROW_NUMBER() OVER(PARTITION BY contact_adw_key, source_system_key, system_of_record, topic_nm, channel, event_cd, event_dtm ORDER BY NULL ASC) AS dupe_check
		FROM (
			select
		  contact_adw_key,source_system_key,system_of_record,topic_nm,channel,event_cd,event_dtm	
			from (
		  SELECT
	      coalesce(contact_N_eff.contact_adw_key, contact.contact_adw_key, '-1') as contact_adw_key,
	      membershipcode_audit.MEMBERSHIP_KY as source_system_key,
		    'MZP' AS system_of_record,
		    (CASE
		        WHEN code='FSC' THEN 'Financial Service'
		        WHEN code='INS' THEN 'Insurance'
		        WHEN code='NMAG' THEN 'Magazines'
		        WHEN code='NMSP' THEN 'Membership promotional'
		        WHEN code='ORP' THEN 'Orphans Outing'
		        WHEN code='TRV' THEN 'Travel'
		        WHEN code='STOP' THEN 'All Mailings'
		      ELSE
		      ''
		    END
		      ) AS topic_nm,
		    (CASE
		        WHEN code='DNDM' THEN 'Mail'
		        WHEN code='DNE' THEN 'Email'
		      ELSE
		      'All'
		    END
		      ) AS channel,
		   'Opt-in' AS event_cd,
		   SAFE_CAST(membershipcode_audit.LAST_UPD_DT AS string) AS event_dtm,
		   'membership' AS src
	     FROM
	       `{{ var.value.INGESTION_PROJECT }}.mzp.membershipcode_audit` membershipcode_audit
			   LEFT JOIN
			       ( select distinct
			  	        mbrship.mbrs_source_system_key as source_1_key, 
			 					mem.contact_adw_key,
			 					mem.effective_start_datetime,
			 					mem.effective_end_datetime
			  	      from (select mbrs_adw_key, mbrs_source_system_key from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` where actv_ind = 'Y') mbrship
			         join (select mbrs_adw_key, contact_adw_key,mbr_source_system_key, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` where actv_ind = 'Y') mem
			 				on mbrship.mbrs_adw_key = mem.mbrs_adw_key
			       ) contact
			 			on safe_cast(membershipcode_audit.MEMBERSHIP_KY as INT64) = contact.source_1_key
			 	LEFT JOIN
			   ( select distinct
			       mbrship.mbrs_source_system_key as source_1_key, 
			 			mem.contact_adw_key,
			 			mem.effective_start_datetime,
			 			mem.effective_end_datetime
			     from (select mbrs_adw_key, mbrs_source_system_key from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` where actv_ind = 'Y') mbrship
			     join (select mbrs_adw_key, contact_adw_key,mbr_source_system_key, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`) mem
			 		on mbrship.mbrs_adw_key = mem.mbrs_adw_key
			   ) contact_N_eff
			 	ON safe_cast(membershipcode_audit.MEMBERSHIP_KY  as INT64) = contact_N_eff.source_1_key and SAFE_CAST(SAFE_CAST(membershipcode_audit.LAST_UPD_DT AS string) as DATETIME) between contact_N_eff.effective_start_datetime and contact_N_eff.effective_end_datetime
		  WHERE
		    membershipcode_audit.code IN( 'DNDM','DNE','FSC','INS','NMAG','NMSP','ORP','TRV','STOP')
			) mscode_contact where 	mscode_contact.contact_adw_key <> '-1'
		  UNION ALL
			select
		  contact_adw_key,source_system_key,system_of_record,topic_nm,channel,event_cd,event_dtm	
			 from (
		  SELECT
        coalesce(con_member_N_eff.contact_adw_key, con_member.contact_adw_key, '-1') as contact_adw_key,
        membercode_audit.member_ky as source_system_key,
		    'MZP' AS system_of_record,
		    '' AS topic_nm,
		    (CASE
		        WHEN code='DNM' THEN 'Mail'
		        WHEN code IN ('DNE',
		        'NOE') THEN 'Email'
		        WHEN code='DNC' THEN 'Phone'
		        WHEN code='DNT' THEN 'Text'
		    END
		    ) AS channel,
	      'Opt-in' AS event_cd,
	      SAFE_CAST(membercode_audit.LAST_UPD_DT AS string) AS event_dtm,
	      'member' AS src
		    FROM
		      `{{ var.value.INGESTION_PROJECT }}.mzp.membercode_audit` membercode_audit
				  LEFT JOIN
				  ( select 
				     contact_adw_key, 
				     source_1_key, 
				     row_number() over(partition by source_1_key order by effective_end_datetime desc) rn 
				    from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` 
				    where key_typ_nm like 'member_key%'  
				  ) con_member	
				  ON membercode_audit.member_ky=con_member.source_1_key and con_member.rn=1
				  LEFT JOIN
					    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` con_member_N_eff
					ON con_member_N_eff.key_typ_nm LIKE 'member_key%' AND membercode_audit.member_ky=con_member_N_eff.source_1_key
					    AND SAFE_CAST(SAFE_CAST(membercode_audit.LAST_UPD_DT AS string) as DATETIME) between con_member_N_eff.effective_start_datetime and con_member_N_eff.effective_end_datetime
		  WHERE
		    membercode_audit.code IN( 'DNC','DNE','NOE','DNM','DNT') 
			) mbrcode_contact where 	mbrcode_contact.contact_adw_key <> '-1'
		) MZP
 ) MZP_INCR
WHERE
  dupe_check=1
  AND
  SAFE_CAST(event_dtm AS DATETIME) > (
  SELECT
    MAX(event_dtm)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_contact_event_optout`
  WHERE
    system_of_record='MZP' )

