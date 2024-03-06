CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_source` AS
SELECT
  comm_ctr_id,
  arch_date AS Archive_dtm,
  sc_dt AS service_call_dtm,
  sc_id AS call_id,
  status_cd AS call_status_cd,
  sc_sts_rsn_cd AS call_status_reason_cd,
  dtl_sts_rsn_cd AS call_status_detail_reason_cd,
  sc_reasgn_rsn_cd AS call_Reassignment_cd,
  call_source AS call_source_cd,
  prob1_cd AS first_problem_cd,
  prob2_cd AS final_problem_cd,
  dtl_prob1_cd AS first_tlc_cd,
  dtl_prob2_cd AS final_tlc_cd,
  sc_veh_manf_yr_dt AS vehicle_year,
  sc_veh_manfr_nm AS vehicle_make,
  sc_veh_mdl_nm AS vehicle_model,
  sc_wait_tm AS promised_wait_tm,
  bl_upars_tx AS loc_breakdown,
  tow_dst_uprs_tx AS tow_dest,
  call_cost AS total_call_cost,
  adw_lake_insert_datetime ,
  ROW_NUMBER() OVER (PARTITION BY comm_ctr_id,sc_dt,sc_id ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.d3.arch_call` AS arch_call
WHERE
  CAST(arch_call.adw_lake_insert_datetime  AS datetime) > (
  SELECT
    MAX(effective_start_datetime )
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`)