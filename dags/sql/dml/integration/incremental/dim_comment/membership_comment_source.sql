CREATE OR REPLACE TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_comment_source`  AS
SELECT
  membership_comment_ky,
  membership_ky,
  create_dt,
  create_user_id,
  comment_type_cd,
  comments,
  assign_user_id,
  resolved_dt,
  resolved_user_id,
  resolution,
  last_upd_dt,
  assign_dept_cd,
  adw_lake_insert_datetime,
  adw_lake_insert_batch_number,
  ROW_NUMBER() OVER (PARTITION BY membership_comment_ky, membership_ky ORDER BY last_upd_dt DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.membership_comment` source;