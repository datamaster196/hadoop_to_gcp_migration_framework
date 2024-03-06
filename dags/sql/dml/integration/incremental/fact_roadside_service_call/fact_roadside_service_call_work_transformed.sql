CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_transformed` AS
SELECT
  *
FROM (
  SELECT
    comm_ctr_id,
    coalesce(csk.contact_adw_key,
      '-1') contact_adw_key,
    '-1' AS product_adw_key,
    coalesce(dim_aca_office.aca_office_adw_key,
      '-1') AS office_CommCenter_adw_key,
    '-1' AS svc_fac_vendor_adw_key,
    '-1' AS ers_Truck_adw_key,
    '-1' AS ers_drvr_adw_key,
    CAST(source.Archive_dtm AS DATETIME) Archive_dtm,
    '-1' AS Archive_adw_key_dt,
    CAST(source.service_call_dtm AS DATETIME) service_call_dtm,
    '-1' AS service_adw_key_dt,
    CAST(source.call_id AS INT64) call_id,
    call_status_cd,
    call_status_desc.LOOK_U_DESC AS call_status_desc,
    call_status_reason_cd,
    call_status_reason_desc.LOOK_U_DESC AS call_status_reason_desc,
    call_status_detail_reason_cd,
    call_status_detail_reason_desc.TLC_DESC AS call_status_detail_reason_desc,
    call_Reassignment_cd,
    call_reassignment_desc.LOOK_U_DESC AS call_reassignment_desc,
    call_source_cd,
    '' AS call_source_desc,
    first_problem_cd,
    first_problem_desc.LOOK_U_DESC AS first_problem_desc,
    final_problem_cd,
    final_problem_desc.LOOK_U_DESC AS final_problem_desc,
    first_tlc_cd,
    first_tlc_desc.TLC_DESC AS first_tlc_desc,
    final_tlc_cd,
    final_tlc_desc.TLC_DESC AS final_tlc_desc,
    vehicle_year,
    vehicle_make,
    vehicle_model,
    CAST(source.promised_wait_tm AS INT64) AS promised_wait_tm,
    loc_breakdown,
    tow_dest,
    total_call_cost,
    '' AS fleet_Adjusted_cost,
    CAST(source.adw_lake_insert_datetime AS datetime) AS effective_start_datetime,
    TO_BASE64(MD5(CONCAT( ifnull(COALESCE(contact_adw_key,
              "-1"),
            ''), '|', ifnull(CAST(-1 AS STRING),
            ''),'|', ifnull(COALESCE(dim_aca_office.aca_office_adw_key,
              "-1"),
            ''), '|', ifnull(CAST(-1 AS STRING),
            ''),'|', ifnull(CAST(-1 AS STRING),
            ''),'|', ifnull(CAST(-1 AS STRING),
            ''),'|', Archive_dtm, ifnull(CAST(-1 AS STRING),
            ''),'|', service_call_dtm, ifnull(CAST(-1 AS STRING),
            ''),'|', ifnull(CAST(call_id AS STRING),
            ''),'|', ifnull(CAST(call_status_cd AS STRING),
            ''),'|', ifnull(CAST(call_status_desc.LOOK_U_DESC AS STRING),
            ''),'|', ifnull(CAST(call_status_reason_cd AS STRING),
            ''),'|', ifnull(CAST(call_status_reason_desc.LOOK_U_DESC AS STRING),
            ''),'|', ifnull(CAST(call_status_detail_reason_cd AS STRING),
            ''),'|', ifnull(CAST(call_status_detail_reason_desc.TLC_DESC AS STRING),
            ''),'|', ifnull(CAST(call_Reassignment_cd AS STRING),
            ''),'|', ifnull(CAST(call_reassignment_desc.LOOK_U_DESC AS STRING),
            ''),'|', ifnull(CAST(call_source_cd AS STRING),
            ''),'|', ifnull(CAST('' AS STRING),
            ''),'|', ifnull(CAST(first_problem_cd AS STRING),
            ''),'|', ifnull(CAST(first_problem_desc.LOOK_U_DESC AS STRING),
            ''),'|', ifnull(CAST(final_problem_cd AS STRING),
            ''),'|', ifnull(CAST(final_problem_desc.LOOK_U_DESC AS STRING),
            ''),'|', ifnull(CAST(first_tlc_cd AS STRING),
            ''),'|', ifnull(CAST(first_tlc_desc.TLC_DESC AS STRING),
            ''),'|', ifnull(CAST(final_tlc_cd AS STRING),
            ''),'|', ifnull(CAST(final_tlc_desc.TLC_DESC AS STRING),
            ''),'|', ifnull(CAST(vehicle_year AS STRING),
            ''),'|', ifnull(CAST(vehicle_make AS STRING),
            ''),'|', ifnull(CAST(vehicle_model AS STRING),
            ''),'|', ifnull(CAST(promised_wait_tm AS STRING),
            ''),'|', ifnull(CAST(loc_breakdown AS STRING),
            ''),'|', ifnull(CAST(tow_dest AS STRING),
            ''),'|', ifnull(CAST(total_call_cost AS STRING),
            ''),'|', ifnull(CAST('' AS STRING),
            ''),'|', '') )) AS adw_row_hash,
    ROW_NUMBER() OVER (PARTITION BY coalesce(dim_aca_office.aca_office_adw_key, '-1'),
      source.service_call_dtm,
      source.call_id
    ORDER BY
      source.adw_lake_insert_datetime DESC) AS dup_check
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_roadside_service_call_work_source` AS source
  LEFT JOIN
    `{{ var.value.INGESTION_PROJECT }}.d3.tlc_code_mapping` call_status_detail_reason_desc
  ON
    call_status_detail_reason_desc.tlc_code=source.call_status_detail_reason_cd
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` csk
  ON
    source.comm_ctr_id = csk.source_1_key
    AND source.call_id = csk.source_2_key
    AND source.service_call_dtm = csk.source_3_key
    AND csk.key_typ_nm LIKE 'service_call_key%'
    AND csk.actv_ind = 'Y'
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` dim_aca_office
  ON
    source.comm_ctr_id = dim_aca_office.rs_asstc_communicator_center
  LEFT OUTER JOIN (
    SELECT
      LOOK_U_DESC,
      LOOK_U_CD
    FROM (
      SELECT
        LOOK_U_DESC,
        LOOK_U_CD,
        ROW_NUMBER() OVER(PARTITION BY look_up.LOOK_U_CD ORDER BY NULL ) AS dupe_check
      FROM
        `{{ var.value.INGESTION_PROJECT }}.d3.look_up` AS look_up)
    WHERE
      dupe_check=1 ) call_status_desc
  ON
    call_status_desc.LOOK_U_CD=source.call_status_cd
  LEFT OUTER JOIN (
    SELECT
      LOOK_U_DESC,
      LOOK_U_CD
    FROM (
      SELECT
        LOOK_U_DESC,
        LOOK_U_CD,
        ROW_NUMBER() OVER(PARTITION BY look_up.LOOK_U_CD ORDER BY NULL ) AS dupe_check
      FROM
        `{{ var.value.INGESTION_PROJECT }}.d3.look_up` AS look_up)
    WHERE
      dupe_check=1 ) call_status_reason_desc
  ON
    call_status_reason_desc.LOOK_U_CD=source.call_status_reason_cd
  LEFT OUTER JOIN (
    SELECT
      LOOK_U_DESC,
      LOOK_U_CD
    FROM (
      SELECT
        LOOK_U_DESC,
        LOOK_U_CD,
        ROW_NUMBER() OVER(PARTITION BY look_up.LOOK_U_CD ORDER BY NULL ) AS dupe_check
      FROM
        `{{ var.value.INGESTION_PROJECT }}.d3.look_up` AS look_up)
    WHERE
      dupe_check=1 ) call_reassignment_desc
  ON
    call_reassignment_desc.LOOK_U_CD=source.call_Reassignment_cd
  LEFT OUTER JOIN (
    SELECT
      LOOK_U_DESC,
      LOOK_U_CD
    FROM (
      SELECT
        LOOK_U_DESC,
        LOOK_U_CD,
        ROW_NUMBER() OVER(PARTITION BY look_up.LOOK_U_CD ORDER BY NULL ) AS dupe_check
      FROM
        `{{ var.value.INGESTION_PROJECT }}.d3.look_up` AS look_up)
    WHERE
      dupe_check=1 ) first_problem_desc
  ON
    first_problem_desc.LOOK_U_CD=source.first_problem_cd
  LEFT OUTER JOIN (
    SELECT
      LOOK_U_DESC,
      LOOK_U_CD
    FROM (
      SELECT
        LOOK_U_DESC,
        LOOK_U_CD,
        ROW_NUMBER() OVER(PARTITION BY look_up.LOOK_U_CD ORDER BY NULL ) AS dupe_check
      FROM
        `{{ var.value.INGESTION_PROJECT }}.d3.look_up` AS look_up)
    WHERE
      dupe_check=1 ) final_problem_desc
  ON
    final_problem_desc.LOOK_U_CD=source.final_problem_cd
  LEFT OUTER JOIN (
    SELECT
      TLC_CODE,
      TLC_DESC
    FROM (
      SELECT
        TLC_CODE,
        TLC_DESC,
        ROW_NUMBER() OVER(PARTITION BY first_tlc_desc.TLC_CODE ORDER BY NULL ) AS dupe_check
      FROM
        `{{ var.value.INGESTION_PROJECT }}.d3.tlc_code_mapping` AS first_tlc_desc)
    WHERE
      dupe_check=1 ) first_tlc_desc
  ON
    source.first_tlc_cd = first_tlc_desc.TLC_CODE
  LEFT OUTER JOIN (
    SELECT
      TLC_CODE,
      TLC_DESC
    FROM (
      SELECT
        TLC_CODE,
        TLC_DESC,
        ROW_NUMBER() OVER(PARTITION BY first_tlc_desc.TLC_CODE ORDER BY NULL ) AS dupe_check
      FROM
        `{{ var.value.INGESTION_PROJECT }}.d3.tlc_code_mapping` AS first_tlc_desc)
    WHERE
      dupe_check=1 ) final_tlc_desc
  ON
    source.final_tlc_cd = final_tlc_desc.TLC_CODE
  WHERE
    source.dupe_check=1) where dup_check=1