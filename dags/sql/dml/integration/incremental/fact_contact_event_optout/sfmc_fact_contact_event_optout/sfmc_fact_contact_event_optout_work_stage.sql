CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.sfmc_fact_contact_event_optout_work_stage` AS
SELECT
  source.contact_event_pref_adw_key,
  source.contact_adw_key,
  source.source_system_key,
  source.system_of_record,
  source.topic_nm,
  source.channel,
  source.event_cd,
  source.event_dtm
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.sfmc_fact_contact_event_optout_work_transform` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_contact_event_optout` target
ON
  source.contact_adw_key=target.contact_adw_key
  AND source.source_system_key=target.source_system_key
  AND source.system_of_record=target.system_of_record
  AND target.system_of_record='SFMC'
WHERE
  target.contact_event_pref_adw_key IS NULL
  OR source.contact_event_pref_adw_key <> target.contact_event_pref_adw_key
GROUP BY
  source.contact_event_pref_adw_key,
  source.contact_adw_key,
  source.source_system_key,
  source.system_of_record,
  source.topic_nm,
  source.channel,
  source.event_cd,
  source.event_dtm