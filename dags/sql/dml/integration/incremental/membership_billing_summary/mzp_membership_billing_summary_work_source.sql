CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_source` AS
SELECT
  billing_summary_source.bill_summary_ky AS bill_summary_source_key,
  billing_summary_source.membership_ky,
  billing_summary_source.process_dt AS bill_process_dt,
  billing_summary_source.notice_nr AS bill_notice_nbr,
  billing_summary_source.bill_at AS bill_amt,
  billing_summary_source.credit_at AS bill_credit_amt,
  billing_summary_source.bill_type,
  billing_summary_source.expiration_dt AS mbr_expiration_dt,
  billing_summary_source.payment_at AS bill_paid_amt,
  billing_summary_source.renew_method_cd AS bill_renewal_method,
  billing_summary_source.bill_panel AS bill_panel_cd,
  CASE
  WHEN billing_summary_source.ebill_fl is null then 'U'
  else billing_summary_source.ebill_fl END AS bill_ebilling_ind,
  billing_summary_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY billing_summary_source.bill_summary_ky ORDER BY billing_summary_source.last_upd_dt DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.bill_summary` AS billing_summary_source
WHERE
  billing_summary_source.last_upd_dt IS NOT NULL
  AND CAST(billing_summary_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`)