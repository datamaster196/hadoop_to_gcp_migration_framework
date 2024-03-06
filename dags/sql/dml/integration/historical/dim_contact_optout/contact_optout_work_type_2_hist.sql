CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_type_2_hist` AS
WITH
  contact_preference_hist AS (
  SELECT
    CONCAT( contact_adw_key, COALESCE(topic_nm,''), channel) AS contact_pref_source_key,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY contact_adw_key, topic_nm, channel ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(cast(effective_start_datetime as datetime)) OVER (PARTITION BY contact_adw_key, topic_nm, channel ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    contact_pref_source_key  ,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    contact_preference_hist 
  ), set_groups AS (
  SELECT
    contact_pref_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY contact_pref_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    contact_pref_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    contact_pref_source_key ,
    adw_row_hash,
    grouping_column 
  )
SELECT
  contact_pref_source_key ,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped