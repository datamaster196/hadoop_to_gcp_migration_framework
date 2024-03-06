INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call` ( rs_service_call_adw_key,
    contact_adw_key,
    product_adw_key,
    office_CommCenter_adw_key,
    svc_fac_vendor_adw_key,
    ers_Truck_adw_key,
    ers_drvr_adw_key,
    Archive_dtm,
    Archive_adw_key_dt,
    service_call_dtm,
    service_adw_key_dt,
    call_id,
    call_status_cd,
    call_status_desc,
    call_status_reason_cd,
    call_status_reason_desc,
    call_status_detail_reason_cd,
    call_status_detail_reason_desc,
    call_Reassignment_cd,
    call_reassignment_desc,
    call_source_cd,
    call_source_desc,
    first_problem_cd,
    first_problem_desc,
    final_problem_cd,
    final_problem_desc,
    first_tlc_cd,
    first_tlc_desc,
    final_tlc_cd,
    final_tlc_desc,
    vehicle_year,
    vehicle_make,
    vehicle_model,
    promised_wait_tm,
    loc_breakdown,
    tow_dest,
    total_call_cost,
    fleet_Adjusted_cost,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  rs_service_call_adw_key,
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
  source.promised_wait_tm,
  source.loc_breakdown,
  source.tow_dest,
  source.total_call_cost,
  source.fleet_Adjusted_cost,
  hist_type_2.effective_start_datetime,
  hist_type_2.effective_end_datetime,
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_transformed` source
ON
  ( source.office_CommCenter_adw_key =hist_type_2.office_CommCenter_adw_key
    AND source.service_call_dtm =hist_type_2.service_call_dtm
    AND source.call_id =hist_type_2.call_id
    AND source.effective_start_datetime=hist_type_2.effective_start_datetime )
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS rs_service_call_adw_key,
    office_CommCenter_adw_key,
    service_call_dtm,
    call_id
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_transformed`
  GROUP BY
    rs_service_call_adw_key,
    office_CommCenter_adw_key,
    service_call_dtm,
    call_id ) pk
ON
  (source.office_CommCenter_adw_key =pk.office_CommCenter_adw_key
    AND source.service_call_dtm =pk.service_call_dtm
    AND source.call_id =pk.call_id );

--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for contact_adw_key
SELECT
  COUNT(target.contact_adw_key ) AS contact_adw_key_count
FROM (
  SELECT
    DISTINCT contact_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT contact_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`) source_FK_1
  WHERE
    target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
IF
  ((contact_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.fact_roadside_service_call product_adw_key . FK Column: contact_adw_key '));

	-- Orphaned foreign key check for product_adw_key
SELECT
  COUNT(target.product_adw_key ) AS product_adw_key_count
FROM (
  SELECT
    DISTINCT product_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call` ) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT product_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`) source_FK_2
  WHERE
    target.product_adw_key = source_FK_2.product_adw_key)
HAVING
IF
  ((product_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.fact_roadside_service_call. FK Column: product_adw_key'));

-- Orphaned foreign key check for office_CommCenter_adw_key
SELECT
  COUNT(target.office_CommCenter_adw_key ) AS office_CommCenter_adw_key_count
FROM (
  SELECT
    DISTINCT office_CommCenter_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT aca_office_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) source_FK_2
  WHERE
    target.office_CommCenter_adw_key = source_FK_2.aca_office_adw_key)
HAVING
IF
  ((office_CommCenter_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.fact_roadside_service_call. FK Column: office_CommCenter_adw_key '));
  -- Orphaned foreign key check for svc_fac_vendor_adw_key
SELECT
  COUNT(target.svc_fac_vendor_adw_key ) AS svc_fac_vendor_adw_key_count
FROM (
  SELECT
    DISTINCT svc_fac_vendor_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT vendor_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor`) source_FK_1
  WHERE
    target.svc_fac_vendor_adw_key = source_FK_1.vendor_adw_key)
HAVING
IF
  ((svc_fac_vendor_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.fact_roadside_service_call. FK Column: svc_fac_vendor_adw_key'));
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    office_CommCenter_adw_key,call_id,service_call_dtm,
    effective_start_datetime,
    COUNT(*) AS dupe_count -- source_system_key needs to be updated
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`
  GROUP BY
    1,
    2,3,4
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.fact_roadside_service_call' ) );
  -------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.rs_service_call_adw_key )
FROM (
  SELECT
    rs_service_call_adw_key,
    office_CommCenter_adw_key,call_id,service_call_dtm,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`) a
JOIN (
  SELECT
    rs_service_call_adw_key,
    office_CommCenter_adw_key,call_id,service_call_dtm,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`) b
ON
  a.rs_service_call_adw_key=b.rs_service_call_adw_key
  AND a.office_CommCenter_adw_key = b.office_CommCenter_adw_key
  AND a.call_id = b.call_id
  AND a.service_call_dtm = b.service_call_dtm
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.rs_service_call_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.fact_roadside_service_call' ));