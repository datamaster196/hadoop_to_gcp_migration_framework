CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_work_type_2_hist` AS
WITH
  payment_summary_hist AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY payment_applied_source_key ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(effective_start_datetime) OVER (PARTITION BY payment_applied_source_key ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_summary_transformed` 
  ), set_grouping_column AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR SAFE_CAST(prev_row_hash as STRING) <> SAFE_CAST(adw_row_hash as STRING) THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    payment_summary_hist 
  ), set_groups AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY payment_applied_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    payment_applied_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    payment_applied_source_key,
    adw_row_hash,
    grouping_column 
  )
SELECT
  payment_applied_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped