CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_work_source` AS
SELECT
  membership_source.billing_cd,
  membership_source.membership_id,
  membership_source.membership_ky,
  membership_source.address_line1,
  membership_source.address_line2,
  membership_source.city,
  membership_source.state,
  membership_source.zip,
  membership_source.delivery_route,
  membership_source.country,
  membership_source.membership_type_cd,
  membership_source.cancel_dt,
  membership_source.status_dt,
  membership_source.phone,
  membership_source.status,
  membership_source.billing_category_cd,
  membership_source.dues_cost_at,
  membership_source.dues_adjustment_at,
  CASE when membership_source.future_cancel_fl is null then 'U'
    else membership_source.future_cancel_fl end as future_cancel_fl,
  membership_source.future_cancel_dt,
  membership_source.oot_fl,
  membership_source.address_change_dt,
  membership_source.adm_d2000_update_dt,
  membership_source.tier_ky,
  membership_source.temp_addr_ind,
  membership_source.previous_membership_id,
  membership_source.previous_club_cd,
  membership_source.transfer_club_cd,
  membership_source.transfer_dt,
  CASE when membership_source.dont_solicit_fl is null then 'U'
  else membership_source.dont_solicit_fl end as dont_solicit_fl,
  CASE when membership_source.bad_address_fl is null then 'U'
  else membership_source.bad_address_fl end as bad_address_fl,
  CASE when membership_source.dont_send_pub_fl is null then 'U'
    else membership_source.dont_send_pub_fl end as dont_send_pub_fl,
  membership_source.market_tracking_cd,
      CASE when membership_source.salvage_fl is null then 'U'
    else membership_source.salvage_fl end as salvage_fl,
  membership_source.coverage_level_cd,
  membership_source.group_cd,
--  membership_source.coverage_level_ky,
  membership_source.ebill_fl,
  membership_source.segmentation_cd,
  membership_source.promo_code,
  membership_source.phone_type,
  membership_source.branch_ky,
  membership_source.last_upd_dt,
  membership_source.unapplied_at,
  membership_source.advance_pay_at,
  membership_source.opp_fl,
  membership_source.carryover_at,
  membership_source.pp_fc_fl,
  membership_source.pp_fc_dt,
  membership_source.switch_primary_fl,
  membership_source.suppress_bill_fl,
  membership_source.offer_end_dt,
  ROW_NUMBER() OVER(PARTITION BY membership_source.membership_id ORDER BY membership_source.last_upd_dt DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.membership` AS membership_source
WHERE
  CAST(membership_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`)