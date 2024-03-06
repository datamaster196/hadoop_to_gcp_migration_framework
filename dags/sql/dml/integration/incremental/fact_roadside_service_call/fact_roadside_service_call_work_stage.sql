CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_stage` AS
SELECT
  COALESCE(target.rs_service_call_adw_key,
    GENERATE_UUID()) rs_service_call_adw_key,
  source.contact_adw_key,
  source.product_adw_key,
  source.office_CommCenter_adw_key,
  source.svc_fac_vendor_adw_key,
  source.ers_Truck_adw_key,
  source.ers_drvr_adw_key,
  source.Archive_dtm,
  source.Archive_adw_key_dt,
  source.service_call_dtm,
  source.service_adw_key_dt,
  source.call_id,
  source.call_status_cd,
  source.call_status_desc,
  source.call_status_reason_cd,
  source.call_status_reason_desc,
  source.call_status_detail_reason_cd,
  source.call_status_detail_reason_desc,
  source.call_Reassignment_cd,
  source.call_reassignment_desc,
  source.call_source_cd,
  source.call_source_desc,
  source.first_problem_cd,
  source.first_problem_desc,
  source.final_problem_cd,
  source.final_problem_desc,
  source.first_tlc_cd,
  source.first_tlc_desc,
  source.final_tlc_cd,
  source.final_tlc_desc,
  source.vehicle_year,
  source.vehicle_make,
  source.vehicle_model,
  source.promised_wait_tm  ,
  source.loc_breakdown,
  source.tow_dest,
  source.total_call_cost,
  source.fleet_Adjusted_cost,
  source.effective_start_datetime,
  CAST('9999-12-31' AS datetime) AS effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} AS integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} AS integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call` target
ON
  ( source.office_CommCenter_adw_key=target.office_CommCenter_adw_key
    AND source.call_id=target.call_id
    AND source.service_call_dtm=target.service_call_dtm
    AND target.actv_ind='Y' )
WHERE
  (target.office_CommCenter_adw_key IS NULL
    AND target.call_id IS NULL
    AND target.service_call_dtm IS NULL)
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.rs_service_call_adw_key,
  target.contact_adw_key,
  target.product_adw_key,
  target.office_CommCenter_adw_key,
  target.svc_fac_vendor_adw_key,
  target.ers_Truck_adw_key,
  target.ers_drvr_adw_key,
  target.Archive_dtm,
  target.Archive_adw_key_dt,
  target.service_call_dtm,
  target.service_adw_key_dt,
  target.call_id,
  target.call_status_cd,
  target.call_status_desc,
  target.call_status_reason_cd,
  target.call_status_reason_desc,
  target.call_status_detail_reason_cd,
  target.call_status_detail_reason_desc,
  target.call_Reassignment_cd,
  target.call_reassignment_desc,
  target.call_source_cd,
  target.call_source_desc,
  target.first_problem_cd,
  target.first_problem_desc,
  target.final_problem_cd,
  target.final_problem_desc,
  target.first_tlc_cd,
  target.first_tlc_desc,
  target.final_tlc_cd,
  target.final_tlc_desc,
  target.vehicle_year,
  target.vehicle_make,
  target.vehicle_model,
  target.promised_wait_tm,
  target.loc_breakdown,
  target.tow_dest,
  target.total_call_cost,
  target.fleet_Adjusted_cost,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 second) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  target.integrate_update_batch_number AS integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call` target
ON
  ( source.office_CommCenter_adw_key=target.office_CommCenter_adw_key
    AND source.call_id=target.call_id
    AND source.service_call_dtm=target.service_call_dtm
    AND target.actv_ind='Y' )
WHERE
  source.adw_row_hash <> target.adw_row_hash ;