 CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_transformed` AS
 SELECT
 coalesce(adw_memship.mbrs_adw_key,'-1') as mbrs_adw_key,
 coalesce(cust_src_key.contact_adw_key,'-1') as contact_adw_key,
 coalesce(solicitation.mbrs_solicit_adw_key,'-1') as mbrs_solicit_adw_key,
 coalesce(role.emp_role_adw_key,'-1') as emp_role_adw_key ,
 member.member_ky as mbr_source_system_key ,
 member.membership_id as mbrs_id ,
 member.associate_id as mbr_assoc_id ,
 member.check_digit_nr as mbr_ck_dgt_nbr,
 member.status as mbr_status_cd ,
 member.status as mbr_extended_status_cd ,
member.member_expiration_dt as mbr_expiration_dt,
member.member_type_cd as prim_mbr_cd,
member.do_not_renew_fl as mbr_do_not_renew_ind ,
member.billing_cd as mbr_billing_cd,
member.billing_category_cd as mbr_billing_category_cd,
member.member_card_expiration_dt as member_card_expiration_dt,
member.status_dt as mbr_status_dtm,
member.cancel_dt as mbr_cancel_dt ,
member.join_aaa_dt as mbr_join_aaa_dt,
member.join_club_dt as mbr_join_club_dt ,
member.reason_joined as mbr_reason_joined_cd ,
member.associate_relation_cd as mbr_assoc_relation_cd ,
member.solicitation_cd as mbr_solicit_cd ,
member.source_of_sale as mbr_source_of_sale_cd,
member.commission_cd as commission_cd,
member.future_cancel_fl as mbr_future_cancel_ind ,
member.future_cancel_dt as mbr_future_cancel_dt ,
member.renew_method_cd as mbr_renew_method_cd,
member.free_member_fl as free_mbr_ind ,
member.previous_membership_id as previous_club_mbrs_id  ,
member.previous_club_cd as member_previous_club_cd,
crossref.previous_member_ky as mbr_merged_previous_key ,
crossref.previous_club_cd as mbr_merged_previous_club_cd ,
crossref.previous_membership_id as mbr_mbrs_merged_previous_id  ,
crossref.previous_associate_id as mbr_assoc_merged_previous_id ,
crossref.previous_check_digit_nr as mbr_merged_previous_ck_dgt_nbr ,
crossref.previous_member16_id as mbr_merged_previous_mbr16_id  ,
member.name_change_dt as mbr_change_nm_dtm ,
member.adm_email_changed_dt as mbr_email_changed_dtm ,
member.bad_email_fl as mbr_bad_email_ind ,
--member.email_optout_fl as mbr_email_optout_ind ,
member.web_last_login_dt as mbr_web_last_login_dtm ,
member.active_expiration_dt as mbr_active_expiration_dt ,
member.activation_dt as mbr_actvtn_dt ,
--member_code.DNT_CODE as mbr_dn_text_ind,
CASE
      WHEN CAST(member_code.DEM_CODE AS STRING) IS NULL THEN 'U'
      WHEN CAST(member_code.DEM_CODE AS STRING)='0' THEN 'N'
      WHEN CAST(member_code.DEM_CODE AS STRING)='1' THEN 'Y'
    ELSE
    CAST(member_code.DEM_CODE AS STRING)
  END
    AS mbr_duplicate_email_ind,
--member_code.DNC_CODE as mbr_dn_call_ind,
--member_code.NOE_CODE as mbr_no_email_ind,
--member_code.DNM_CODE as mbr_dn_mail_ind,
CASE
      WHEN CAST(member_code.DNAE_CODE AS STRING) IS NULL THEN 'U'
      WHEN CAST(member_code.DNAE_CODE AS STRING)='0' THEN 'N'
      WHEN CAST(member_code.DNAE_CODE AS STRING)='1' THEN 'Y'
    ELSE
    CAST(member_code.DNAE_CODE AS STRING)
  END
    AS mbr_dn_ask_for_email_ind,
--member_code.DNE_CODE as mbr_dn_email_ind,
CASE
      WHEN CAST(member_code.RFE_CODE AS STRING) IS NULL THEN 'U'
      WHEN CAST(member_code.RFE_CODE AS STRING)='0' THEN 'N'
      WHEN CAST(member_code.RFE_CODE AS STRING)='1' THEN 'Y'
    ELSE
    CAST(member_code.RFE_CODE AS STRING)
  END
    AS mbr_refused_give_email_ind,
member.mbrs_purge_ind as mbrs_purge_ind,
CAST (member.last_upd_dt AS datetime) AS last_upd_dt,
member.pp_fc_dt as payment_plan_future_cancel_dt,
member.pp_fc_fl as payment_plan_future_cancel_ind,
credential.credential_cd as mbr_card_typ_cd,
credential.description as mbr_card_typ_desc,
member.entitlement_start_dt as entitlement_start_dt,
member.entitlement_end_dt as entitlement_end_dt,
member.previous_expiration_dt as mbr_previous_expiration_dt,
TO_BASE64(MD5(CONCAT(ifnull(member.membership_id,''),'|',
ifnull(member.associate_id,''),'|',
ifnull(member.check_digit_nr,''),'|',
ifnull(member.status,''),'|',
ifnull(member.member_expiration_dt,''),'|',
ifnull(member.member_type_cd,''),'|',
ifnull(member.do_not_renew_fl,''),'|',
ifnull(member.billing_cd,''),'|',
ifnull(member.billing_category_cd,''),'|',
ifnull(member.member_card_expiration_dt,''),'|',
ifnull(member.status_dt,''),'|',
ifnull(member.cancel_dt,''),'|',
ifnull(member.join_aaa_dt,''),'|',
ifnull(member.join_club_dt,''),'|',
ifnull(member.reason_joined,''),'|',
ifnull(member.associate_relation_cd,''),'|',
ifnull(member.solicitation_cd,''),'|',
ifnull(member.source_of_sale,''),'|',
ifnull(member.commission_cd,''),'|',
ifnull(member.future_cancel_fl,''),'|',
ifnull(member.future_cancel_dt,''),'|',
ifnull(member.renew_method_cd,''),'|',
ifnull(member.free_member_fl,''),'|',
ifnull(member.previous_membership_id,''),'|',
ifnull(member.previous_club_cd,''),'|',
ifnull(crossref.previous_member_ky,''),'|',
ifnull(crossref.previous_club_cd,''),'|',
ifnull(crossref.previous_membership_id,''),'|',
ifnull(crossref.previous_associate_id,''),'|',
ifnull(crossref.previous_check_digit_nr,''),'|',
ifnull(crossref.previous_member16_id,''),'|',
ifnull(member.name_change_dt,''),'|',
ifnull(member.adm_email_changed_dt,''),'|',
ifnull(member.bad_email_fl,''),'|',
--ifnull(member.email_optout_fl,''),'|',
ifnull(member.web_last_login_dt,''),'|',
ifnull(member.active_expiration_dt,''),'|',
ifnull(member.activation_dt,''),'|',
ifnull(adw_memship.mbrs_adw_key,'-1'),'|',
ifnull(cust_src_key.contact_adw_key,'-1'),'|',
ifnull(solicitation.mbrs_solicit_adw_key,'-1'),'|',
ifnull(role.emp_role_adw_key,'-1'),'|',
--ifnull(SAFE_CAST(member_code.DNT_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.DEM_CODE AS STRING),''),'|',
--ifnull(SAFE_CAST(member_code.DNC_CODE AS STRING),''),'|',
--ifnull(SAFE_CAST(member_code.NOE_CODE AS STRING),''),'|',
--ifnull(SAFE_CAST(member_code.DNM_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.DNAE_CODE AS STRING),''),'|',
--ifnull(SAFE_CAST(member_code.DNE_CODE AS STRING),''),'|',
ifnull(SAFE_CAST(member_code.RFE_CODE AS STRING),''),'|',
ifnull(member.mbrs_purge_ind,''),'|',
ifnull(member.pp_fc_dt,''),'|',
ifnull(member.pp_fc_fl,''),'|',
ifnull(credential.credential_cd,''),'|',
ifnull(credential.description,''),'|',
ifnull(member.entitlement_start_dt,''),'|',
ifnull(member.entitlement_end_dt,''),'|',
ifnull(member.previous_expiration_dt,''),'|'
))) as adw_row_hash

FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_source` AS member
    LEFT JOIN
    (
    SELECT
      member_ky,
     -- MAX(CASE
     --     WHEN code='DNT' THEN 1
     --   ELSE
     --   0
     -- END
     --   ) AS DNT_CODE,
      MAX(CASE
          WHEN code='DEM' THEN 1
        ELSE
        0
      END
        ) AS DEM_CODE,
    --  MAX(CASE
    --      WHEN code='DNC' THEN 1
    --    ELSE
    --   0
    --  END
    --    ) AS DNC_CODE,
    --  MAX(CASE
    --      WHEN code='NOE' THEN 1
    --    ELSE
    --    0
    --  END
    --    ) AS NOE_CODE,
    --  MAX(CASE
    --      WHEN code='DNM' THEN 1
    --    ELSE
    --    0
    --  END
    --    ) AS DNM_CODE,
      MAX(CASE
          WHEN code='DNAE' THEN 1
        ELSE
        0
      END
        ) AS DNAE_CODE,
    --  MAX(CASE
    --      WHEN code='DNE' THEN 1
    --    ELSE
    --    0
    --  END
    --    ) AS DNE_CODE,
      MAX(CASE
          WHEN code='RFE' THEN 1
        ELSE
        0
      END
        ) AS RFE_CODE
    FROM
      `{{ var.value.INGESTION_PROJECT }}.mzp.member_code`
    GROUP BY
      1) AS member_code
  ON
    member.member_ky = member_code.member_ky
    LEFT JOIN
      (SELECT mbrs_source_system_key,
              mbrs_adw_key,
              ROW_NUMBER() OVER (PARTITION BY mbrs_source_system_key) AS DUPE_CHECK
      FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`
      WHERE actv_ind='Y'
      ) adw_memship
      ON member.membership_ky =SAFE_CAST(adw_memship.mbrs_source_system_key AS STRING) AND adw_memship.DUPE_CHECK=1
left join
(select 
       contact_adw_key, 
       source_1_key, 
       row_number() over(partition by source_1_key order by effective_end_datetime desc) rn 
       from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` 
       where key_typ_nm like 'member_key%' ) cust_src_key
on member.member_ky=cust_src_key.source_1_key and cust_src_key.rn=1
left join ( select member_ky,
previous_member_ky,
previous_club_cd,
previous_membership_id,
previous_associate_id,
previous_check_digit_nr,
previous_member16_id,
ROW_NUMBER() OVER(PARTITION BY member_ky ORDER BY NULL DESC) AS DUPE_CHECK
from
`{{ var.value.INGESTION_PROJECT }}.mzp.member_crossref`) crossref
on crossref.member_ky=member.member_ky and crossref.DUPE_CHECK=1
LEFT JOIN
`{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role` role
on member.agent_id=role.emp_role_id and role.actv_ind='Y'
LEFT JOIN
(SELECT solicit_cd,
mbrs_solicit_adw_key,
ROW_NUMBER() OVER (PARTITION BY solicit_cd) AS DUPE_CHECK
FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation`
WHERE actv_ind='Y'
) solicitation
ON solicitation.solicit_cd=member.solicitation_cd and solicitation.DUPE_CHECK=1
LEFT JOIN
(
SELECT
    credential_ky,
    credential_cd,
    description,
    ROW_NUMBER() OVER (PARTITION BY credential_ky ORDER BY LAST_UPD_DT DESC ) DUPE_CHECK
FROM `{{ var.value.INGESTION_PROJECT }}.mzp.credential`) credential
on member.credential_ky = credential.credential_ky
    AND credential.DUPE_CHECK=1
where member.dupe_check=1