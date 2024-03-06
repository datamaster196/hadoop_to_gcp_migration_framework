CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_stage` AS
SELECT
    COALESCE(target.mbr_rider_adw_key,GENERATE_UUID()) mbr_rider_adw_key,
SAFE_CAST(source.mbrs_adw_key as STRING) mbrs_adw_key,
SAFE_CAST(source.mbr_adw_key as STRING) mbr_adw_key,
source.mbr_ar_card_adw_key ,
source.emp_role_adw_key,
source.product_adw_key,
SAFE_CAST(source.mbr_rider_source_key AS INT64) mbr_rider_source_key,
source.mbr_rider_status_cd,
SAFE_CAST(source.mbr_rider_status_dtm as DATETIME) mbr_rider_status_dtm,
SAFE_CAST(TIMESTAMP(source.mbr_rider_cancel_dt) as DATE) mbr_rider_cancel_dt,
SAFE_CAST(TIMESTAMP(source.mbr_rider_cost_effective_dt) as DATE) mbr_rider_cost_effective_dt,
source.mbr_rider_solicit_cd,
source.mbr_rider_do_not_renew_ind,
SAFE_CAST(source.mbr_rider_dues_cost_amt as NUMERIC) mbr_rider_dues_cost_amt,
SAFE_CAST(source.mbr_rider_dues_adj_amt as NUMERIC) mbr_rider_dues_adj_amt,
SAFE_CAST(source.mbr_rider_payment_amt as NUMERIC) mbr_rider_payment_amt ,
source.mbr_rider_paid_by_cd,
SAFE_CAST(TIMESTAMP(source.mbr_rider_future_cancel_dt) as DATE) mbr_rider_future_cancel_dt,
source.mbr_rider_billing_category_cd,
source.mbr_rider_cancel_reason_cd,
source.mbr_rider_cancel_reason_desc,
source.mbr_rider_reinstate_ind,
SAFE_CAST(TIMESTAMP(source.mbr_rider_effective_dt) as DATE) mbr_rider_effective_dt,
SAFE_CAST(source.mbr_rider_original_cost_amt as NUMERIC) mbr_rider_original_cost_amt,
source.mbr_rider_reinstate_reason_cd,
SAFE_CAST(source.mbr_rider_extend_exp_amt as NUMERIC) mbr_rider_extend_exp_amt,
SAFE_CAST(source.mbr_rider_actual_cancel_dtm as DATETIME) mbr_rider_actual_cancel_dtm ,
SAFE_CAST(source.mbr_rider_def_key as INT64) mbr_rider_def_key,
SAFE_CAST(TIMESTAMP(source.mbr_rider_actvtn_dt) as DATE) mbr_rider_actvtn_dt,
last_upd_dt AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS actv_ind,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    {{ dag_run.id }} integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    {{ dag_run.id }} integrate_update_batch_number
from
 `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_transformed` source
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider` target
  ON
    (SAFE_CAST(source.mbr_rider_source_key as INT64)=target.mbr_rider_source_key
      AND target.actv_ind='Y')
  WHERE
    target.mbr_rider_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
  SELECT
   target.mbr_rider_adw_key	,
target.mbrs_adw_key	,
target.mbr_adw_key	,
target.mbr_ar_card_adw_key,
target.emp_role_adw_key	,
target.product_adw_key	,
target.mbr_rider_source_key	,
target.mbr_rider_status_cd	,
target.mbr_rider_status_dtm	,
target.mbr_rider_cancel_dt		,
target.mbr_rider_cost_effective_dt	,
target.mbr_rider_solicit_cd	,
target.mbr_rider_do_not_renew_ind	,
target.mbr_rider_dues_cost_amt,
target.mbr_rider_dues_adj_amt	,
target.mbr_rider_payment_amt,
target.mbr_rider_paid_by_cd	,
target.mbr_rider_future_cancel_dt	,
target.mbr_rider_billing_category_cd	,
target.mbr_rider_cancel_reason_cd	,
target.mbr_rider_cancel_reason_desc,
target.mbr_rider_reinstate_ind	,
target.mbr_rider_effective_dt		,
target.mbr_rider_original_cost_amt,
target.mbr_rider_reinstate_reason_cd	,
target.mbr_rider_extend_exp_amt,
target.mbr_rider_actual_cancel_dtm	,
target.mbr_rider_def_key			,
target.mbr_rider_actvtn_dt		,
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
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_transformed` source
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider` target
  ON
    (SAFE_CAST(source.mbr_rider_source_key as  INT64)=target.mbr_rider_source_key
      AND target.actv_ind='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash