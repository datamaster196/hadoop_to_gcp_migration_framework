-----------------------------------------------------------------------------------------------------------------
----------- This query filters the latest record for  --------------------------------
-----------  different event_cd amongst data in incremental and target table ---------------------------


CREATE OR REPLACE TABLE  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_stage` AS
SELECT 
   COALESCE(target.contact_pref_adw_key ,GENERATE_UUID()) contact_pref_adw_key ,
  source. contact_adw_key ,
  source.topic_nm ,
  source.channel ,
  source.event_cd ,
  SAFE_CAST(source.effective_start_datetime as DATETIME) as effective_start_datetime ,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout` target
ON
  (CONCAT(source.contact_adw_key, COALESCE(source.topic_nm,''), source.channel )=CONCAT(target.contact_adw_key, COALESCE(target.topic_nm,''), target.channel )
    AND target.actv_ind='Y')
WHERE
  (CONCAT(target.contact_adw_key,COALESCE(target.topic_nm,''), target.channel ) IS NULL
  OR source.adw_row_hash <> target.adw_row_hash) AND source.desc_dupe_channel=1
  UNION ALL
SELECT
  contact_pref_adw_key,
  target.contact_adw_key ,
  target.topic_nm ,
  target.channel ,
  target.event_cd ,
  target.effective_start_datetime,
  DATETIME_SUB(cast(source.effective_start_datetime as datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  {{ dag_run.id }} integrate_update_batch_number
 FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout` target
ON
  (CONCAT(source.contact_adw_key, COALESCE(source.topic_nm,''), source.channel )=CONCAT(target.contact_adw_key,COALESCE(target.topic_nm,''), target.channel )
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash AND source.desc_dupe_channel=1