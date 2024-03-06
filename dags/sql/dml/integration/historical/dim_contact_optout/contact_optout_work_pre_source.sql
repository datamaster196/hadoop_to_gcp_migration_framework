--------------------------------------------------------------------------------------------------------------------------------------
----------- This query picks the incremental records from the source and filter Exact dupilcates  records ----------------------------
----------- on basis of same channel, topic,event_cd and event date/time stamp and eleminate them.  ----------------------------------


CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_source` AS
SELECT
  contact_adw_key,
  topic_nm,
  channel,
  event_cd,
  event_dtm,
  dupe_channel,
  LAG(dupe_channel) OVER(PARTITION BY contact_adw_key, topic_nm, channel, event_cd ORDER BY channel, event_dtm ASC) AS prev_dupe_channel
FROM (
  SELECT
    contact_adw_key,
    topic_nm,
    channel,
    event_cd,
    event_dtm,
    ROW_NUMBER() OVER(PARTITION BY contact_pref_source.contact_adw_key, contact_pref_source.topic_nm, contact_pref_source.channel, contact_pref_source.event_cd, contact_pref_source.event_dtm ORDER BY NULL) AS dupe_check,

    ROW_NUMBER() OVER(PARTITION BY contact_pref_source.contact_adw_key, contact_pref_source.topic_nm, contact_pref_source.channel ORDER BY contact_pref_source.event_dtm ASC) AS dupe_channel
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_contact_event_optout` AS contact_pref_source)
WHERE
  dupe_check=1;  