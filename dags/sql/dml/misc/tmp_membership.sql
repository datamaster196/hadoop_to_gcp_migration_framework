  --insert records from backup to New table
  insert into `{{ var.value.INGESTION_PROJECT }}.mzp.membership` (
  membership_ky
  ,membership_id
  ,status
  ,billing_cd
  ,status_dt
  ,cancel_dt
  ,billing_category_cd
  ,dues_cost_at
  ,dues_adjustment_at
  ,unapplied_at
  ,refund_at
  ,credits_at
  ,advance_pay_at
  ,branch_ky
  ,incentive_type_cd
  ,incentive_fulfillment_dt
  ,drop_card_rqt_dt
  ,drop_card_prt_dt
  ,expiration_extn_mths
  ,future_cancel_fl
  ,future_cancel_dt
  ,address_line1
  ,address_line2
  ,city
  ,state
  ,zip
  ,country
  ,oot_fl
  ,address_change_dt
  ,bill_addr_ind
  ,temp_addr_ind
  ,unlisted_fl
  ,phone
  ,previous_membership_id
  ,previous_club_cd
  ,send_card_to
  ,send_bill_to
  ,gift_type_cd
  ,last_upd_dt
  ,sponsor_ky
  ,split_fl
  ,transfer_club_cd
  ,transfer_reason_cd
  ,transfer_dt
  ,other_phone_fl
  ,dont_solicit_fl
  ,bad_address_fl
  ,dont_send_pub_fl
  ,fleet_account_fl
  ,attention_line
  ,driver_ct
  ,market_tracking_cd
  ,recommended_by_id
  ,donor_renewal_fl
  ,gift_kit_given_fl
  ,hold_bill_fl
  ,safety_fund_applied_fl
  ,lone_primary_dt
  ,lone_primary_ct
  ,roll_on_fl
  ,delivery_route
  ,no_safety_fund_fl
  ,hold_bill_comment
  ,adm_d2000_update_dt
  ,salvage_fl
  ,no_refunds_fl
  ,referred_by
  ,referred_by_membership_ky
  ,referred_by_user_id
  ,lead_ky
  ,coverage_level_cd
  ,flex_string_1
  ,flex_string_2
  ,flex_string_3
  ,flex_string_4
  ,flex_string_5
  ,flex_string_6
  ,flex_string_7
  ,flex_string_8
  ,flex_string_9
  ,flex_string_10
  ,flex_number_1
  ,flex_number_2
  ,flex_number_3
  ,flex_number_4
  ,flex_number_5
  ,flex_date_1
  ,flex_date_2
  ,flex_date_3
  ,flex_date_4
  ,flex_date_5
  ,group_cd
  ,coverage_level_ky
  ,membership_type_cd
  ,excluded_unapplied_at
  ,excluded_reason_cd
  ,billing_split_cd
  ,salvage_detail_ky
  ,mrm_extract_update_dt
  ,wk_address_1
  ,wk_address_2
  ,wk_city
  ,wk_state
  ,wk_zip
  ,wk_country
  ,ebill_email
  ,ebill_fl
  ,ebill_last_upd_dt
  ,terms_fl
  ,terms_last_upd_dt
  ,segmentation_cd
  ,segmentation_setup_ky
  ,address_validation_code
  ,activation_dt
  ,promo_code
  ,tier_ky
  ,phone_type
  ,adw_lake_insert_datetime
  ,adw_lake_insert_batch_number
  )
  select * from `{{ var.value.INGESTION_PROJECT }}.work.membership_backup`;

--Update membership table
UPDATE  `{{ var.value.INGESTION_PROJECT }}.mzp.membership` m
SET m.opp_fl = m2.opp_fl,
   m.carryover_at = m2.carryover_at,
   m.pp_fc_fl = m2.pp_fc_fl,
   m.pp_fc_dt =  m2.pp_fc_dt,
   m.switch_primary_fl = m2.switch_primary_fl,
   m.suppress_bill_fl  = m2.suppress_bill_fl ,
   m.offer_end_dt = m2.offer_end_dt
from (select membership_ky, max(adw_lake_insert_datetime) as adw_lake_insert_datetime
      from  `{{ var.value.INGESTION_PROJECT }}.mzp.membership`
      group by membership_ky) m1, `{{ var.value.INGESTION_PROJECT }}.work.mz_membership` m2
WHERE m.membership_ky = m1.membership_ky
and m.adw_lake_insert_datetime = m1.adw_lake_insert_datetime
and m1.membership_ky = m2.membership_ky;
