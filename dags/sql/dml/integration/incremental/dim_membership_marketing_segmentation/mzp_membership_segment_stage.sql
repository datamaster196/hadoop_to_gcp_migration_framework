CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_stage` AS
SELECT
  COALESCE(target.mbrs_marketing_segmntn_adw_key, GENERATE_UUID()) mbrs_marketing_segmntn_adw_key
  ,source.mbrs_adw_key
  ,source.member_expiration_dt
  ,source.segmntn_test_group_cd
  ,source.segmntn_panel_cd
  ,source.segmntn_control_panel_ind
  ,source.segmntn_comm_cd
  ,source.segment_nm
  ,source.adw_row_hash
  ,source.effective_start_datetime
  ,source.effective_end_datetime
  ,source.actv_ind
  ,source.integrate_insert_datetime
  ,source.integrate_insert_batch_number
  ,source.integrate_update_datetime
  ,source.integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_mbrs_marketing_segmntn` target
ON
  (source.segment_key=concat(ifnull(target.mbrs_adw_key,''),'|', SAFE_CAST(target.mbr_expiration_dt AS STRING), '|', ifnull(target.segment_nm,''), '|', ifnull(target.segmntn_panel_cd,''))
    AND target.actv_ind='Y')
WHERE
  target.mbrs_marketing_segmntn_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
  target.mbrs_marketing_segmntn_adw_key
  ,target.mbrs_adw_key
  ,target.mbr_expiration_dt
  ,target.segmntn_test_group_cd
  ,target.segmntn_panel_cd
  ,target.segmntn_control_panel_ind
  ,target.segmntn_comm_cd
  ,target.segment_nm
  ,target.adw_row_hash
  ,target.effective_start_datetime
  ,DATETIME_SUB(SAFE_CAST(source.effective_start_datetime as datetime),
    INTERVAL 1 second) AS effective_end_datetime
  ,'N' AS actv_ind
  ,target.integrate_insert_datetime
  ,target.integrate_insert_batch_number
  ,CURRENT_DATETIME()
  ,1
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_mbrs_marketing_segmntn` target
ON
  (source.segment_key=concat(ifnull(target.mbrs_adw_key,''),'|', SAFE_CAST(target.mbr_expiration_dt AS STRING), '|', ifnull(target.segment_nm,''), '|', ifnull(target.segmntn_panel_cd,''))
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash