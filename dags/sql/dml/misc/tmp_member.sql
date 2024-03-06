  --insert records from backup to New table
  insert into `{{ var.value.INGESTION_PROJECT }}.mzp.member` (
  member_ky
  ,billing_category_cd
  ,member_card_expiration_dt
  ,member_expiration_dt
  ,membership_ky
  ,membership_id
  ,associate_id
  ,check_digit_nr
  ,member_type_cd
  ,status
  ,billing_cd
  ,status_dt
  ,cancel_dt
  ,join_aaa_dt
  ,join_club_dt
  ,reason_joined
  ,birth_dt
  ,salutation
  ,first_name
  ,middle_name
  ,last_name
  ,name_suffix
  ,gender
  ,associate_relation_cd
  ,do_not_renew_fl
  ,solicitation_cd
  ,source_of_sale
  ,commission_cd
  ,agent_id
  ,duplicate_sticker_ct
  ,duplicate_sticker_req_dt
  ,duplicate_sticker_prt_dt
  ,incentive_type_cd
  ,incentive_fulfillment_dt
  ,change_dt
  ,future_cancel_fl
  ,future_cancel_dt
  , free_member_fl
  ,email
  ,last_upd_dt
  ,renew_method_cd
  ,previous_membership_id
  ,previous_club_cd
  ,sponsor_ky
  ,extend_reason_cd
  ,extend_cycles_ct
  ,send_card_to
  ,send_bill_to
  ,gift_kit_given_fl
  ,name_change_dt
  ,adm_email_changed_dt
  ,card_rollback_dt
  ,adm_do_not_reinstate_fl
  ,original_agent_id
  ,card_on_renewal_fl
  ,sticker_count
  ,bad_email_fl
  ,autorenewal_dt
  ,email_optout_fl
  ,extend_charge_fl
  ,solicitation_cd_extra_months
  ,last_cycle_dt
  ,group_cd
  ,lead_sequence_nr
  ,autorenew_failure_ct
  ,web_username
  ,web_password
  ,web_last_login_dt
  ,web_password_set_dt
  ,web_invalid_attempt_ct
  ,web_disabled_fl
  ,web_question_cd
  ,web_answer
  ,web_password_must_set_fl
  ,customer_id
  ,active_expiration_dt
  ,graduation_yr
  ,renew_method_at_renewal
  ,corp_join_dt
  ,activation_dt
  ,adw_lake_insert_datetime
  ,adw_lake_insert_batch_number
  )
  select * from `{{ var.value.INGESTION_PROJECT }}.work.member_backup`;

--Update member table
UPDATE  `{{ var.value.INGESTION_PROJECT }}.mzp.member` m
SET m.pp_fc_fl = m2.pp_fc_fl,
   m.pp_fc_dt =  m2.pp_fc_dt,
   m.credential_ky = m2.credential_ky,
   m.entitlement_start_dt  = m2.entitlement_start_dt ,
   m.entitlement_end_dt = m2.entitlement_end_dt,
   m.previous_expiration_dt = m2.previous_expiration_dt
from (select member_ky, max(adw_lake_insert_datetime) as adw_lake_insert_datetime
      from  `{{ var.value.INGESTION_PROJECT }}.work.member`
      group by member_ky) m1, `{{ var.value.INGESTION_PROJECT }}.work.mz_member` m2
WHERE m.member_ky = m1.member_ky
and m.adw_lake_insert_datetime = m1.adw_lake_insert_datetime
and m1.member_ky = m2.member_ky;
