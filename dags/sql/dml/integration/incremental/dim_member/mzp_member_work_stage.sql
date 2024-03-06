CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_stage` AS
SELECT
COALESCE(target.mbr_adw_key,GENERATE_UUID()) AS mbr_adw_key,
source.mbrs_adw_key mbrs_adw_key,
source.contact_adw_key contact_adw_key,
source.mbrs_solicit_adw_key,
source.emp_role_adw_key emp_role_adw_key,
SAFE_CAST(source.mbr_source_system_key AS INT64) mbr_source_system_key,
source.mbrs_id  mbrs_id ,
source.mbr_assoc_id  mbr_assoc_id ,
SAFE_CAST(source.mbr_ck_dgt_nbr AS INT64) mbr_ck_dgt_nbr,
source.mbr_status_cd mbr_status_cd,
source.mbr_status_cd mbr_extended_status_cd,
CAST(substr(source.mbr_expiration_dt,0,10) AS date) as mbr_expiration_dt,
CAST(substr(source.member_card_expiration_dt,0,10) AS date) as  member_card_expiration_dt,
source.prim_mbr_cd prim_mbr_cd,
source.mbr_do_not_renew_ind mbr_do_not_renew_ind,
source.mbr_billing_cd mbr_billing_cd,
source.mbr_billing_category_cd mbr_billing_category_cd,
CAST(source.mbr_status_dtm as datetime) as mbr_status_dtm,
CAST(substr(source.mbr_cancel_dt,0,10) AS date) as mbr_cancel_dt,
CASE WHEN SUBSTR(source.mbr_join_aaa_dt,1,3) IN ('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
    THEN parse_date('%h %d %Y',substr(source.mbr_join_aaa_dt,0,11))
    ELSE parse_date('%d-%b-%y',substr(source.mbr_join_aaa_dt,0,10))
    END as mbr_join_aaa_dt,
CAST(substr(source.mbr_join_club_dt,0,10) AS date) as mbr_join_club_dt,
source.mbr_reason_joined_cd mbr_reason_joined_cd,
source.mbr_assoc_relation_cd mbr_assoc_relation_cd,
source.mbr_solicit_cd mbr_solicit_cd,
source.mbr_source_of_sale_cd mbr_source_of_sale_cd,
source.commission_cd commission_cd,
source.mbr_future_cancel_ind mbr_future_cancel_ind,
CAST(substr(source.mbr_future_cancel_dt,0,10) AS date) as mbr_future_cancel_dt,
source.mbr_renew_method_cd mbr_renew_method_cd,
source.free_mbr_ind free_mbr_ind,
source.previous_club_mbrs_id  previous_club_mbrs_id ,
source.member_previous_club_cd member_previous_club_cd,
safe_cast(source.mbr_merged_previous_key as int64) mbr_merged_previous_key,
source.mbr_merged_previous_club_cd mbr_merged_previous_club_cd,
source.mbr_mbrs_merged_previous_id  mbr_mbrs_merged_previous_id ,
source.mbr_assoc_merged_previous_id mbr_assoc_merged_previous_id,
source.mbr_merged_previous_ck_dgt_nbr mbr_merged_previous_ck_dgt_nbr,
source.mbr_merged_previous_mbr16_id  mbr_merged_previous_mbr16_id ,
SAFE_CAST(source.mbr_change_nm_dtm AS DATETIME) AS mbr_change_nm_dtm,
SAFE_CAST(source.mbr_email_changed_dtm AS DATETIME) mbr_email_changed_dtm,
source.mbr_bad_email_ind mbr_bad_email_ind,
--source.mbr_email_optout_ind mbr_email_optout_ind,
SAFE_CAST(source.mbr_web_last_login_dtm AS DATETIME) mbr_web_last_login_dtm,
CAST(substr (source.mbr_active_expiration_dt,0,10) AS date) AS mbr_active_expiration_dt,
CAST(substr (source.mbr_actvtn_dt,0,10) AS date) AS mbr_actvtn_dt,
--SAFE_CAST(source.mbr_dn_text_ind AS STRING) mbr_dn_text_ind,
SAFE_CAST(source.mbr_duplicate_email_ind AS STRING) mbr_duplicate_email_ind,
--SAFE_CAST(source.mbr_dn_call_ind AS STRING) mbr_dn_call_ind,
--SAFE_CAST(source.mbr_no_email_ind AS STRING) mbr_no_email_ind,
--SAFE_CAST(source.mbr_dn_mail_ind AS STRING) mbr_dn_mail_ind,
SAFE_CAST(source.mbr_dn_ask_for_email_ind AS STRING) mbr_dn_ask_for_email_ind,
--SAFE_CAST(source.mbr_dn_email_ind AS STRING) mbr_dn_email_ind,
SAFE_CAST(source.mbr_refused_give_email_ind AS STRING) mbr_refused_give_email_ind,
SAFE_CAST(source.mbrs_purge_ind AS STRING) mbrs_purge_ind,
CAST(substr(source.payment_plan_future_cancel_dt, 0, 10) AS date) as payment_plan_future_cancel_dt,
source.payment_plan_future_cancel_ind ,
source.mbr_card_typ_cd ,
source.mbr_card_typ_desc,
CAST(substr(source.entitlement_start_dt, 0, 10) AS date) as entitlement_start_dt,
CAST(substr(source.entitlement_end_dt, 0, 10) AS date) as entitlement_end_dt,
CAST(substr(source.mbr_previous_expiration_dt, 0, 10) AS date) as mbr_previous_expiration_dt,
source.last_upd_dt AS effective_start_datetime,
CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS actv_ind,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    {{ dag_run.id }} integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    {{ dag_run.id }} integrate_update_batch_number

 FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_transformed` source
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` target
  ON
    (source.mbr_source_system_key =SAFE_CAST(target.mbr_source_system_key AS STRING)
      AND target.actv_ind='Y')
  WHERE
    target.mbr_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
target.mbr_adw_key,
target.mbrs_adw_key,
target.contact_adw_key,
target.mbrs_solicit_adw_key,
target.emp_role_adw_key,
target.mbr_source_system_key,
target.mbrs_id ,
target.mbr_assoc_id ,
target.mbr_ck_dgt_nbr,
target.mbr_status_cd,
target.mbr_extended_status_cd,
target.mbr_expiration_dt,
target.mbr_card_expiration_dt ,
target.prim_mbr_cd,
target.mbr_do_not_renew_ind,
target.mbr_billing_cd,
target.mbr_billing_category_cd,
target.mbr_status_dtm,
target.mbr_cancel_dt,
target.mbr_join_aaa_dt,
target.mbr_join_club_dt,
target.mbr_reason_joined_cd,
target.mbr_assoc_relation_cd,
target.mbr_solicit_cd,
target.mbr_source_of_sale_cd,
target.mbr_comm_cd ,
target.mbr_future_cancel_ind,
target.mbr_future_cancel_dt,
target.mbr_renew_method_cd,
target.free_mbr_ind,
target.previous_club_mbrs_id ,
target.previous_club_cd,
target.mbr_merged_previous_key,
target.mbr_merged_previous_club_cd,
target.mbr_mbrs_merged_previous_id ,
target.mbr_assoc_merged_previous_id,
target.mbr_merged_previous_ck_dgt_nbr,
target.mbr_merged_previous_mbr16_id ,
target.mbr_change_nm_dtm,
target.mbr_email_changed_dtm,
target.mbr_bad_email_ind,
--target.mbr_email_optout_ind,
target.mbr_web_last_login_dtm,
target.mbr_active_expiration_dt,
target.mbr_actvtn_dt,
--target.mbr_dn_text_ind,
target.mbr_duplicate_email_ind,
--target.mbr_dn_call_ind,
--target.mbr_no_email_ind,
--target.mbr_dn_mail_ind,
target.mbr_dn_ask_for_email_ind,
--target.mbr_dn_email_ind,
target.mbr_refused_give_email_ind,
target.mbrs_purge_ind,
target.payment_plan_future_cancel_dt,
target.payment_plan_future_cancel_ind,
target.mbr_card_typ_cd,
target.mbr_card_typ_desc,
target.entitlement_start_dt,
target.entitlement_end_dt,
target.mbr_previous_expiration_dt,
target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS actv_ind,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    target.integrate_update_batch_number
FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_transformed` source
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` target
  ON
    (source.mbr_source_system_key =SAFE_CAST(target.mbr_source_system_key AS STRING)
      AND target.actv_ind='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash