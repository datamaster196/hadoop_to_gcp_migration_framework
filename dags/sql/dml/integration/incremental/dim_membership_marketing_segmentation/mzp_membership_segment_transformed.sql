CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_transformed` as
SELECT
  mbrs_adw_key,
  member_expiration_dt,
  segmntn_test_group_cd,
  segmntn_panel_cd,
  segmntn_control_panel_ind,
  segmntn_comm_cd,
  segment_nm,
  segment_key,
  CURRENT_DATETIME()   as effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} AS integrate_insert_batch_number,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} AS integrate_update_batch_number,
  adw_row_hash
FROM (
  SELECT
    mbrs_adw_key,
    member_expiration_dt,
    segmntn_test_group_cd,
    segmntn_panel_cd,
    segmntn_control_panel_ind,
    segmntn_comm_cd,
    segment_nm,
    segment_key,
    adw_row_hash,
    ROW_NUMBER() OVER(PARTITION BY mbrs_adw_key, member_expiration_dt, segmntn_panel_cd, segment_nm ORDER BY NULL DESC) AS dupe_check
  FROM (
    SELECT
      COALESCE(membership.mbrs_adw_key,
        '-1') AS mbrs_adw_key,
      SAFE_CAST(SAFE_CAST(membership_exp_dt AS datetime) AS date) AS member_expiration_dt,
      panel_cd AS segmntn_test_group_cd,
      segmentation_panel_cd AS segmntn_panel_cd,
      control_panel_fl AS segmntn_control_panel_ind,
      commission_cd AS segmntn_comm_cd,
      name AS segment_nm,
      CONCAT(ifnull(membership.mbrs_adw_key,
          '-1'),'|', ifnull(SAFE_CAST(SAFE_CAST(SAFE_CAST(membership_exp_dt AS datetime) AS date) AS STRING),
          ''), '|', ifnull(name,
          ''), '|', ifnull(segmentation_panel_cd,
          '')) AS segment_key,
      TO_BASE64(MD5(CONCAT(COALESCE( source.panel_cd,
              ''),COALESCE( source.control_panel_fl,
              ''),COALESCE( source.commission_cd,
              '')))) AS adw_row_hash
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_source` AS source
    LEFT JOIN
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` membership
    ON
      (source.membership_ky=SAFE_CAST(membership.mbrs_source_system_key AS STRING)
        AND membership.actv_ind='Y')
    WHERE
      source.dupe_check=1 ) )
WHERE
  dupe_check=1



