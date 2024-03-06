INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member` (
    mbr_adw_key
    ,mbrs_adw_key
    ,contact_adw_key
    ,mbrs_solicit_adw_key
    ,emp_role_adw_key
    ,mbr_source_system_key
    ,mbrs_id
    ,mbr_assoc_id
    ,mbr_ck_dgt_nbr
    ,mbr_status_cd
    ,mbr_extended_status_cd
    ,mbr_expiration_dt
    ,mbr_card_expiration_dt
    ,prim_mbr_cd
    ,mbr_do_not_renew_ind
    ,mbr_billing_cd
    ,mbr_billing_category_cd
    ,mbr_status_dtm
    ,mbr_cancel_dt
    ,mbr_join_aaa_dt
    ,mbr_join_club_dt
    ,mbr_reason_joined_cd
    ,mbr_assoc_relation_cd
    ,mbr_solicit_cd
    ,mbr_source_of_sale_cd
    ,mbr_comm_cd
    ,mbr_future_cancel_ind
    ,mbr_future_cancel_dt
    ,mbr_renew_method_cd
    ,free_mbr_ind
    ,previous_club_mbrs_id
    ,previous_club_cd
    ,mbr_merged_previous_key
    ,mbr_merged_previous_club_cd
    ,mbr_mbrs_merged_previous_id
    ,mbr_assoc_merged_previous_id
    ,mbr_merged_previous_ck_dgt_nbr
    ,mbr_merged_previous_mbr16_id
    ,mbr_change_nm_dtm
    ,mbr_email_changed_dtm
    ,mbr_bad_email_ind
    --,mbr_email_optout_ind
    ,mbr_web_last_login_dtm
    ,mbr_active_expiration_dt
    ,mbr_actvtn_dt
    --,mbr_dn_text_ind
    ,mbr_duplicate_email_ind
    --,mbr_dn_call_ind
    --,mbr_no_email_ind
    --,mbr_dn_mail_ind
    ,mbr_dn_ask_for_email_ind
    --,mbr_dn_email_ind
    ,mbr_refused_give_email_ind
    ,mbrs_purge_ind
    ,payment_plan_future_cancel_dt
    ,payment_plan_future_cancel_ind
    ,mbr_card_typ_cd
    ,mbr_card_typ_desc
    ,entitlement_start_dt
    ,entitlement_end_dt
    ,mbr_previous_expiration_dt
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
)
VALUES
(
    '-1'                   --mbr_adw_key
    ,'-1'                  --mbrs_adw_key
    ,'-1'                  --contact_adw_key
    ,'-1'                  --mbrs_solicit_adw_key
    ,'-1'                  --emp_role_adw_key
    ,-1                    --mbr_source_system_key
    ,'UNKNOWN'             --mbrs_id
    ,'UNKNOWN'             --mbr_assoc_id
    ,-1                    --mbr_ck_dgt_nbr
    ,'UNKNOWN'             --mbr_status_cd
    ,'UNKNOWN'             --mbr_extended_status_cd
    ,'1900-01-01'          --mbr_expiration_dt
    ,'1900-01-01'          --mbr_card_expiration_dt
    ,'U'             --prim_mbr_cd
    ,'U'             --mbr_do_not_renew_ind
    ,'UNKNOWN'             --mbr_billing_cd
    ,'UNKNOWN'             --mbr_billing_category_cd
    ,'1900-01-01'          --mbr_status_dtm
    ,'1900-01-01'          --mbr_cancel_dt
    ,'1900-01-01'          --mbr_join_aaa_dt
    ,'1900-01-01'          --mbr_join_club_dt
    ,'UNKNOWN'             --mbr_reason_joined_cd
    ,'UNKNOWN'             --mbr_assoc_relation_cd
    ,'UNKNOWN'             --mbr_solicit_cd
    ,'UNKNOWN'             --mbr_source_of_sale_cd
    ,'UNKNOWN'             --mbr_comm_cd
    ,'U'             --mbr_future_cancel_ind
    ,'1900-01-01'          --mbr_future_cancel_dt
    ,'UNKNOWN'             --mbr_renew_method_cd
    ,'U'             --free_mbr_ind
    ,'UNKNOWN'             --previous_club_mbrs_id
    ,'UNKNOWN'             --previous_club_cd
    ,-1                    --mbr_merged_previous_key
    ,'UNKNOWN'             --mbr_merged_previous_club_cd
    ,'UNKNOWN'             --mbr_mbrs_merged_previous_id
    ,'UNKNOWN'             --mbr_assoc_merged_previous_id
    ,'UNKNOWN'             --mbr_merged_previous_ck_dgt_nbr
    ,'UNKNOWN'             --mbr_merged_previous_mbr16_id
    ,'1900-01-01'          --mbr_change_nm_dtm
    ,'1900-01-01'          --mbr_email_changed_dtm
    ,'U'             --mbr_bad_email_ind
    --,'UNKNOWN'             --mbr_email_optout_ind
    ,'1900-01-01'          --mbr_web_last_login_dtm
    ,'1900-01-01'          --mbr_active_expiration_dt
    ,'1900-01-01'          --mbr_actvtn_dt
    --,'UNKNOWN'             --mbr_dn_text_ind
    ,'U'             --mbr_duplicate_email_ind
    --,'UNKNOWN'             --mbr_dn_call_ind
    --,'UNKNOWN'             --mbr_no_email_ind
    --,'UNKNOWN'             --mbr_dn_mail_ind
    ,'UNKNOWN'             --mbr_dn_ask_for_email_ind
    --,'UNKNOWN'             --mbr_dn_email_ind
    ,'UNKNOWN'             --mbr_refused_give_email_ind
    ,'U'             --mbrs_purge_ind
    ,'1900-01-01'          --payment_plan_future_cancel_dt
    ,'UNKNOWN'             --payment_plan_future_cancel_ind
    ,'UNKNOWN'             --mbr_card_typ_cd
    ,'UNKNOWN'             --mbr_card_typ_desc
    ,'1900-01-01'          --entitlement_start_dt
    ,'1900-01-01'          --entitlement_end_dt
    ,'1900-01-01'          --mbr_previous_expiration_dt
    ,'1900-01-01'          --effective_start_datetime
    ,'9999-12-31'          --effective_end_datetime
    ,'N'                   --actv_ind
    ,'-1'                  --adw_row_hash
    ,CURRENT_DATETIME      --integrate_insert_datetime
    ,1                     --integrate_insert_batch_number
    ,CURRENT_DATETIME      --ntegrate_update_datetime
    ,1                     --integrate_update_batch_number
);