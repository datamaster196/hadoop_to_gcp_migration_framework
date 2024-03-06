CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.member_fact_contact_event_optout_work_transform` AS

  SELECT
    TO_BASE64(MD5(CONCAT(ifnull(channel,
            ''),'|',ifnull(event_dtm,
            ''),'|',ifnull(event_cd,
            ''),'|',ifnull(source_system_key,
            ''),'|',ifnull(system_of_record,
            '')))) AS contact_event_pref_adw_key,
    contact_adw_key,
    source_system_key,
    system_of_record,
    topic_nm,
    channel,
    event_cd,
    PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',
      event_dtm ) AS event_dtm
FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.member_fact_contact_event_optout_work_source`
WHERE dupe_check=1
