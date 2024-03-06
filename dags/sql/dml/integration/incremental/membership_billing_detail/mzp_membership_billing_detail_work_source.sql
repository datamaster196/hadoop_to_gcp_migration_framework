CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_detail_work_source` AS
 select billing_detail_source.bill_detail_ky as bill_detail_source_key,
 billing_detail_source.bill_summary_ky as bill_summary_source_key,
 billing_detail_source.member_ky ,
 billing_detail_source.rider_ky ,
 billing_detail_source.membership_fees_ky,
 billing_detail_source.bill_detail_at as bill_detail_amt,
 billing_detail_source.detail_text as bill_detail_text,
 billing_detail_source.billing_category_cd as rider_billing_category_cd,
 billing_detail_source.solicitation_cd as rider_solicit_cd,
 billing_detail_source.last_upd_dt,
 ROW_NUMBER() OVER(PARTITION BY billing_detail_source.bill_detail_ky ORDER BY billing_detail_source.last_upd_dt DESC) AS dupe_check
   FROM
   `{{ var.value.INGESTION_PROJECT }}.mzp.bill_detail` AS billing_detail_source
WHERE
  billing_detail_source.last_upd_dt IS NOT NULL AND
  CAST(billing_detail_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail`)