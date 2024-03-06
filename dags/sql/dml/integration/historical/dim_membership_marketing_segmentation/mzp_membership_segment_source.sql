CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_source` AS
SELECT
  membership_ky,
  historysource.segmentation_setup_ky,
  membership_exp_dt,
  historysource.segmentation_test_end_dt,
  historysource.segmentation_schedule_dt,
  panel_cd,
  segmentation_panel_cd,
  control_panel_fl,
  commission_cd,
  setupsource.name AS name,
  ROW_NUMBER() OVER(PARTITION BY historysource.membership_ky, historysource.membership_exp_dt, historysource.segmentation_panel_cd, setupsource.name ORDER BY segmentation_history_ky DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.segmentation_history` AS historysource
JOIN (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY segmentation_setup_ky ORDER BY segmentation_test_end_dt DESC) AS rn
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.segmentation_setup` ) setupsource
ON
  (historysource.segmentation_setup_ky=setupsource.segmentation_setup_ky
    AND setupsource.rn=1)
