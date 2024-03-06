CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_source` AS
SELECT
comm_ctr_id,
arch_date	as	Archive_dtm	,
sc_dt	as	service_call_dtm	,
sc_id	as	call_id	,
status_cd	as	call_status_cd	,
sc_sts_rsn_cd	as	call_status_reason_cd	,
dtl_sts_rsn_cd	as	call_status_detail_reason_cd	,
sc_reasgn_rsn_cd	as	call_Reassignment_cd	,
call_source	as	call_source_cd	,
prob1_cd	as	first_problem_cd	,
prob2_cd	as	final_problem_cd	,
dtl_prob1_cd	as	first_tlc_cd	,
dtl_prob2_cd	as	final_tlc_cd	,
sc_veh_manf_yr_dt	as	vehicle_year	,
sc_veh_manfr_nm	as	vehicle_make	,
sc_veh_mdl_nm	as	vehicle_model	,
sc_wait_tm	as	promised_wait_tm	,
bl_upars_tx	as	loc_breakdown	,
tow_dst_uprs_tx	as	tow_dest	,
call_cost	as	total_call_cost	,
ROW_NUMBER() OVER (PARTITION BY comm_ctr_id, sc_dt,sc_id  ORDER BY arch_date DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.d3.arch_call` as arch_call
