CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_stage` AS
    SELECT
    COALESCE(target.mbrs_payment_received_adw_key,
      GENERATE_UUID()) mbrs_payment_received_adw_key,
      source.mbrs_adw_key mbrs_adw_key,
source.mbrs_billing_summary_adw_key mbrs_billing_summary_adw_key ,
source.mbr_ar_card_adw_key mbr_ar_card_adw_key ,
source.aca_office_adw_key aca_office_adw_key ,
source.emp_adw_key emp_adw_key ,
SAFE_CAST(source.BATCH_PAYMENT_KY AS INT64) payment_received_source_key ,
SAFE_CAST(source.BATCH_KY AS INT64) payment_received_batch_key ,
source.BATCH_NAME payment_received_nm,
SAFE_CAST(source.EXP_CT AS INT64) payment_received_cnt ,
SAFE_CAST(source.EXP_AT AS NUMERIC) payment_received_expected_amt ,
SAFE_CAST(source.CREATE_DT AS DATETIME) payment_received_created_dtm ,
source.STATUS payment_received_status ,
SAFE_CAST(source.STATUS_DT AS DATETIME) payment_received_status_dtm ,
SAFE_CAST(source.PAYMENT_AT AS NUMERIC) payment_received_amt ,
source.PAYMENT_METHOD_CD payment_received_method_cd ,
source.payment_received_method_desc payment_received_method_desc ,
source.PAYMENT_SOURCE_CD payment_source_cd ,
source.payment_source_desc payment_source_desc ,
source.TRANSACTION_TYPE_CD payment_typ_cd ,
source.payment_typ_desc payment_typ_desc ,
source.REASON_CD payment_reason_cd ,
source.PAID_BY_CD payment_paid_by_desc ,
source.ADJUSTMENT_DESCRIPTION_CD payment_adj_cd ,
source.payment_adj_desc payment_adj_desc ,
source.POST_COMPLETED_FL payment_received_post_ind ,
SAFE_CAST(source.POST_DT AS DATETIME) payment_received_post_dtm ,
source.AUTO_RENEW_FL payment_received_ar_ind ,
      SAFE_CAST(last_upd_dt AS DATETIME) AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS actv_ind,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    {{ dag_run.id }} integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    {{ dag_run.id }} integrate_update_batch_number
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_transformed` source
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received` target
  ON
    (source.batch_payment_ky=SAFE_CAST(target.payment_received_source_key AS STRING)
      AND target.actv_ind='Y')
  WHERE
    target.mbrs_payment_received_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
    UNION ALL
    SELECT
    target.mbrs_payment_received_adw_key,
target.mbrs_adw_key,
target.mbrs_billing_summary_adw_key,
target.mbr_ar_card_adw_key,
target.aca_office_adw_key,
target.emp_adw_key,
target.payment_received_source_key,
target.payment_received_batch_key,
target.payment_received_nm,
target.payment_received_cnt,
target.payment_received_expected_amt,
target.payment_received_created_dtm,
target.payment_received_status,
target.payment_received_status_dtm,
target.payment_received_amt,
target.payment_received_method_cd,
target.payment_received_method_desc,
target.payment_source_cd,
target.payment_source_desc,
target.payment_typ_cd,
target.payment_typ_desc,
target.payment_reason_cd,
target.payment_paid_by_desc,
target.payment_adj_cd,
target.payment_adj_desc,
target.payment_received_post_ind,
target.payment_received_post_dtm,
target.payment_received_ar_ind,
target.effective_start_datetime,
     DATETIME_SUB(SAFE_CAST(source.last_upd_dt AS DATETIME),
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS actv_ind,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    target.integrate_update_batch_number
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_transformed` source
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received` target
  ON
    (source.batch_payment_ky=SAFE_CAST(target.payment_received_source_key AS STRING)
      AND target.actv_ind='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash