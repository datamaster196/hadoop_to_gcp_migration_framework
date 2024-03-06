INSERT INTO `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership` (
    mbrs_adw_key
    ,address_adw_key
    ,phone_adw_key
    ,product_adw_key
    ,aca_office_adw_key
    ,mbrs_source_system_key
    ,mbrs_id
    ,mbrs_status_cd
    ,mbrs_billing_cd
    ,mbrs_status_dtm
    ,mbrs_cancel_dt
    ,mbrs_billing_category_cd
    ,mbrs_dues_cost_amt
    ,mbrs_dues_adj_amt
    ,mbrs_future_cancel_ind
    ,mbrs_future_cancel_dt
    ,mbrs_out_of_territory_cd
    ,mbrs_address_change_dtm
    ,mbrs_cdx_export_update_dtm
    ,mbrs_tier_key
    ,mbrs_temporary_address_ind
    ,previous_club_mbrs_id
    ,previous_club_cd
    ,mbrs_transfer_club_cd
    ,mbrs_transfer_dtm
    ,mbrs_merged_previous_club_cd
    ,mbrs_mbrs_merged_previous_id
    ,mbrs_mbrs_merged_previous_key
    ,mbrs_dn_solicit_ind
    ,mbrs_bad_address_ind
    ,mbrs_dn_send_pubs_ind
    ,mbrs_market_tracking_cd
    ,mbrs_salvage_ind
    ,mbrs_salvaged_ind
    ,mbrs_dn_salvage_ind
    ,mbrs_group_cd
    ,mbrs_typ_cd
    ,mbrs_ebill_ind
    ,mbrs_marketing_segmntn_cd
    ,mbrs_promo_cd
    ,mbrs_phone_typ_cd
    ,mbrs_ers_abuser_ind
    --,mbrs_dn_send_aaawld_ind
    ,mbrs_shadow_mcycle_ind
    ,mbrs_student_ind
    ,mbrs_address_ncoa_update_ind
    ,mbrs_assigned_retention_ind
    ,mbrs_mbrs_no_promotion_ind
    --,mbrs_dn_telemkt_ind
    ,mbrs_dn_offer_plus_ind
    ,mbrs_aaawld_online_ed_ind
    ,mbrs_shadowrv_coverage_ind
    --,mbrs_dn_direct_mail_slc_ind
    ,mbrs_heavy_ers_ind
    ,mbrs_purge_ind
    ,mbrs_unapplied_amt
    ,mbrs_advance_payment_amt
    ,mbrs_balance_amt
    ,payment_plan_future_cancel_dt
    ,payment_plan_future_cancel_ind
    ,switch_prim_ind
    ,supress_bill_ind
    ,offer_end_dt
    ,old_payment_plan_ind
    ,carry_over_amt
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
    '-1'                            --mbrs_adw_key
    ,'-1'                            --address_adw_key
    ,'-1'                            --phone_adw_key
    ,'-1'                            --product_adw_key
    ,'-1'                            --aca_office_adw_key
    ,-1                              --mbrs_source_system_key
    ,'UNKNOWN'                       --mbrs_id
    ,'UNKNOWN'                       --mbrs_status_cd
    ,'UNKNOWN'                       --mbrs_billing_cd
    ,'1900-01-01'                    --mbrs_status_dtm
    ,'1900-01-01'                    --mbrs_cancel_dt
    ,'UNKNOWN'                       --mbrs_billing_category_cd
    ,0                               --mbrs_dues_cost_amt
    ,0                               --mbrs_dues_adj_amt
    ,'U'                       --mbrs_future_cancel_ind
    ,'1900-01-01'                    --mbrs_future_cancel_dt
    ,'UNKNOWN'                       --mbrs_out_of_territory_cd
    ,'1900-01-01'                    --mbrs_address_change_dtm
    ,'1900-01-01'                    --mbrs_cdx_export_update_dtm
    ,-1                              --mbrs_tier_key
    ,'U'                       --mbrs_temporary_address_ind
    ,'UNKNOWN'                       --previous_club_mbrs_id
    ,'UNKNOWN'                       --previous_club_cd
    ,'UNKNOWN'                       --mbrs_transfer_club_cd
    ,'1900-01-01'                    --mbrs_transfer_dtm
    ,'UNKNOWN'                       --mbrs_merged_previous_club_cd
    ,'UNKNOWN'                       --mbrs_mbrs_merged_previous_id
    ,-1                              --mbrs_mbrs_merged_previous_key
    ,'U'                       --mbrs_dn_solicit_ind
    ,'U'                       --mbrs_bad_address_ind
    ,'U'                       --mbrs_dn_send_pubs_ind
    ,'UNKNOWN'                       --mbrs_market_tracking_cd
    ,'U'                       --mbrs_salvage_ind
    ,'UNKNOWN'                       --mbrs_salvaged_ind
    ,'UNKNOWN'                       --mbrs_dn_salvage_ind
    ,'UNKNOWN'                       --mbrs_group_cd
    ,'UNKNOWN'                       --mbrs_typ_cd
    ,'U'                       --mbrs_ebill_ind
    ,'UNKNOWN'                       --mbrs_marketing_segmntn_cd
    ,'UNKNOWN'                       --mbrs_promo_cd
    ,'UNKNOWN'                       --mbrs_phone_typ_cd
    ,'UNKNOWN'                       --mbrs_ers_abuser_ind
    --,'UNKNOWN'                       --mbrs_dn_send_aaawld_ind
    ,'UNKNOWN'                       --mbrs_shadow_mcycle_ind
    ,'UNKNOWN'                       --mbrs_student_ind
    ,'UNKNOWN'                       --mbrs_address_ncoa_update_ind
    ,'UNKNOWN'                       --mbrs_assigned_retention_ind
    ,'UNKNOWN'                       --mbrs_mbrs_no_promotion_ind
    --,'UNKNOWN'                       --mbrs_dn_telemkt_ind
    ,'UNKNOWN'                       --mbrs_dn_offer_plus_ind
    ,'UNKNOWN'                       --mbrs_aaawld_online_ed_ind
    ,'UNKNOWN'                       --mbrs_shadowrv_coverage_ind
    --,'UNKNOWN'                       --mbrs_dn_direct_mail_slc_ind
    ,'UNKNOWN'                       --mbrs_heavy_ers_ind
    ,'U'                       --mbrs_purge_ind
    ,0                               --mbrs_unapplied_amt
    ,0                               --mbrs_advance_payment_amt
    ,0                               --mbrs_balance_amt
    ,'1900-01-01'                    --payment_plan_future_cancel_dt
    ,'UNKNOWN'                       --payment_plan_future_cancel_ind
    ,'UNKNOWN'                       --switch_prim_ind
    ,'UNKNOWN'                       --supress_bill_ind
    ,'1900-01-01'                    --offer_end_dt
    ,'UNKNOWN'                       --old_payment_plan_ind
    ,0                               --carry_over_amt
    ,'1900-01-01'                    --effective_start_datetime
    ,'9999-12-31'                    --effective_end_datetime
    ,'N'                             --actv_ind
    ,'-1'                            --adw_row_hash
    ,CURRENT_DATETIME                --integrate_insert_datetime
    ,1                               --integrate_insert_batch_number
    ,CURRENT_DATETIME                --integrate_update_datetime
    ,1                               --integrate_update_batch_number
);