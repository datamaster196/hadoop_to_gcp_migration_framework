CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_transformed` AS
SELECT
  adw_memship.mbrs_adw_key AS mbrs_adw_key,
  billing_summary_source.bill_summary_source_key,
  billing_summary_source.bill_process_dt,
  billing_summary_source.bill_notice_nbr,
  billing_summary_source.bill_amt,
  billing_summary_source.bill_credit_amt,
  billing_summary_source.bill_type as bill_typ ,
  billing_summary_source.mbr_expiration_dt,
  billing_summary_source.bill_paid_amt,
  billing_summary_source.bill_renewal_method,
  billing_summary_source.bill_panel_cd,
  billing_summary_source.bill_ebilling_ind,
  CAST (billing_summary_source.last_upd_dt AS datetime) AS last_upd_dt,
  TO_BASE64(MD5(CONCAT(ifnull(adw_memship.mbrs_adw_key,
          ''),'|', ifnull(billing_summary_source.bill_summary_source_key,
          ''),'|', ifnull(billing_summary_source.bill_process_dt,
          ''),'|', ifnull(billing_summary_source.bill_notice_nbr,
          ''),'|', ifnull(billing_summary_source.bill_amt,
          ''),'|', ifnull(billing_summary_source.bill_credit_amt,
          ''),'|', ifnull(billing_summary_source.bill_type,
          ''),'|', ifnull(billing_summary_source.mbr_expiration_dt,
          ''),'|', ifnull(billing_summary_source.bill_paid_amt,
          ''),'|', ifnull(billing_summary_source.bill_renewal_method,
          ''),'|', ifnull(billing_summary_source.bill_panel_cd,
          ''),'|', ifnull(billing_summary_source.bill_ebilling_ind,
          ''),'|' ))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_billing_summary_work_source` AS billing_summary_source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` adw_memship
ON
  billing_summary_source.membership_ky =SAFE_CAST(adw_memship.mbrs_source_system_key AS STRING)
  AND actv_ind='Y'
WHERE
  dupe_check = 1