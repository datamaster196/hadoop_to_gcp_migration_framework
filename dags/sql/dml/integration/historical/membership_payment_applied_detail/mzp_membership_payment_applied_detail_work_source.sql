CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_pay_app_detail_work_source` AS
SELECT
  membership_source.member_ky,
  membership_source.membership_fees_ky,
  membership_source.rider_ky,
  membership_source.membership_payment_ky,
  membership_source.payment_detail_ky,
  membership_source.membership_payment_at,
  membership_source.unapplied_used_at,
  membership_source.discount_history_ky,
  membership_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY membership_source.payment_detail_ky, membership_source.last_upd_dt ORDER BY NULL DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.payment_detail` AS membership_source