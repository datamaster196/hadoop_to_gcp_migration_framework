INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_contact_event_optout`
  ( contact_event_pref_adw_key,
  contact_adw_key,
  source_system_key,
  system_of_record,
  topic_nm,
  channel,
  event_cd,
  event_dtm,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number
  )
SELECT
  contact_event_pref_adw_key,
  contact_adw_key,
  source_system_key,
  system_of_record,
  topic_nm,
  channel,
  event_cd,
  safe_cast(event_dtm as datetime) as event_dtm,
  CURRENT_DATETIME() AS integrate_insert_datetime,
  {{ dag_run.id }} as integrate_insert_batch_number ,
  CURRENT_DATETIME() AS integrate_update_datetime,
  {{ dag_run.id }} as integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.aaacom_fact_contact_event_optout_work_transform`
WHERE
  dupe_check=1