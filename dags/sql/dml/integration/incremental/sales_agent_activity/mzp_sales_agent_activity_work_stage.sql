CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_work_stage` AS
SELECT
  COALESCE(target.saa_adw_key,
    GENERATE_UUID()) saa_adw_key,
  source.mbr_adw_key,
  source.mbrs_adw_key,
  source.mbrs_solicit_adw_key,
  source.aca_sales_office,
  source.aca_membership_office_adw_key,
  source.emp_role_adw_key,
  source.product_adw_key,
  source.mbrs_payment_apd_dtl_adw_key,
  source.saa_source_nm,
  source.saa_source_key,
  source.saa_tran_dt,
  source.mbr_comm_cd,
  source.mbr_comm_desc,
  source.mbr_rider_cd,
  source.mbr_rider_desc,
  source.mbr_expiration_dt,
  source.rider_solicit_cd,
  source.rider_source_of_sale_cd,
  source.sales_agent_tran_cd,
  source.saa_dues_amt,
  source.saa_daily_billed_ind,
  source.rider_billing_category_cd,
  source.rider_billing_category_desc,
  source.mbr_typ_cd,
  source.saa_commsable_activity_ind,
  source.saa_add_on_ind,
  source.saa_ar_ind,
  source.saa_fee_typ_cd,
  source.safety_fund_donation_ind ,
  source.saa_payment_plan_ind,
  source.zip_cd,
  source.saa_daily_cnt_ind,
  source.daily_cnt,
  source.last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity` target
ON
  (source.saa_source_key=target.saa_source_key
    AND target.actv_ind='Y')
WHERE
  target.saa_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.saa_adw_key,
  target.mbr_adw_key,
  target.mbrs_adw_key,
  target.mbrs_solicit_adw_key,
  target.aca_sales_office,
  target.aca_membership_office_adw_key,
  target.emp_role_adw_key,
  target.product_adw_key,
  target.mbrs_payment_apd_dtl_adw_key,
  target.saa_source_nm,
  target.saa_source_key,
  target.saa_tran_dt,
  target.mbr_comm_cd,
  target.mbr_comm_desc,
  target.mbr_rider_cd,
  target.mbr_rider_desc,
  target.mbr_expiration_dt,
  target.rider_solicit_cd,
  target.rider_source_of_sale_cd,
  target.sales_agent_tran_cd,
  target.saa_dues_amt,
  target.saa_daily_billed_ind,
  target.rider_billing_category_cd,
  target.rider_billing_category_desc,
  target.mbr_typ_cd,
  target.saa_commsable_activity_ind,
  target.saa_add_on_ind,
  target.saa_ar_ind,
  target.saa_fee_typ_cd,
  target.safety_fund_donation_ind ,
  target.saa_payment_plan_ind,
  target.zip_cd,
  target.saa_daily_cnt_ind,
  target.daily_cnt,
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity` target
ON
  (source.saa_source_key=target.saa_source_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash