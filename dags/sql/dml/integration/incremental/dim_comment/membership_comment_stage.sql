CREATE OR REPLACE TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_comment_stage` AS
SELECT
  COALESCE(b.comment_adw_key,GENERATE_UUID()) AS comment_adw_key,
  a.created_emp_adw_key,
  a.resolved_emp_adw_key,
  a.comment_relation_adw_key,
  a.comment_source_system_nm,
  a.comment_source_system_key,
  a.comment_creation_dtm,
  a.comment_text,
  a.comment_resolved_dtm,
  a.comment_resltn_text,
  a.effective_start_datetime,
  CAST('9999-12-31' AS datetime) AS effective_end_datetime,
  'Y' AS actv_ind,
  current_datetime AS integrate_insert_datetime,
  {{ dag_run.id }} as integrate_insert_batch_number,
  current_datetime AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number,
  a.adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_comment_fkey` a
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_comment` b ON (
   a.comment_source_system_key = b.comment_source_system_key
   AND b.actv_ind='Y')
WHERE
  b.comment_relation_adw_key IS NULL
  OR a.adw_row_hash <> b.adw_row_hash
UNION ALL
SELECT
  b.comment_adw_key AS comment_adw_key,
  b.created_emp_adw_key,
  b.resolved_emp_adw_key,
  b.comment_relation_adw_key,
  b.comment_source_system_nm,
  b.comment_source_system_key,
  b.comment_creation_dtm,
  b.comment_text,
  b.comment_resolved_dtm,
  b.comment_resltn_text,
  b.effective_start_datetime,
  DATETIME_SUB(a.effective_start_datetime, INTERVAL 1 second) AS effective_end_datetime,
  'N' AS actv_ind,
  b.integrate_insert_datetime AS integrate_insert_datetime,
  b.integrate_insert_batch_number AS integrate_insert_batch_number,
  current_datetime AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number,
  b.adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.membership_comment_fkey` a
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_comment` b ON (
    a.comment_source_system_key = b.comment_source_system_key
    AND b.actv_ind = 'Y')
WHERE a.adw_row_hash <> b.adw_row_hash;