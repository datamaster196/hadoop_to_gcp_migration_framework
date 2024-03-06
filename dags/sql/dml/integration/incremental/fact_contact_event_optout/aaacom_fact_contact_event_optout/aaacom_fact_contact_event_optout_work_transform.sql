CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.aaacom_fact_contact_event_optout_work_transform` AS

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
  PARSE_DATETIME('%m/%d/%Y %r',event_dtm ) AS event_dtm,
  ROW_NUMBER() OVER(PARTITION BY contact_adw_key, source_system_key, system_of_record, topic_nm, channel, event_cd, event_dtm  ORDER BY NULL) AS dupe_check

FROM (
  SELECT
    contact_adw_key,
    source_system_key,
    system_of_record,
    topic_nm,
    'Email' AS channel,
    CASE
      WHEN OptOut_byEmail = 'False' THEN 'Opt-in'
      WHEN OptOut_byEmail = 'True' THEN 'Opt-out'
  END
    AS event_cd,
    event_dtm
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.aaacom_fact_contact_event_optout_work_source`
  WHERE
    dupe_check=1
  UNION ALL
  SELECT
    contact_adw_key,
    source_system_key,
    system_of_record,
    topic_nm,
    'Mail' AS channel,
    CASE
      WHEN OptOut_byMail = 'False' THEN 'Opt-in'
      WHEN OptOut_byMail = 'True' THEN 'Opt-out'
  END
    AS event_cd,
    event_dtm
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.aaacom_fact_contact_event_optout_work_source`
  WHERE
    dupe_check=1
  UNION ALL
  SELECT
    contact_adw_key,
    source_system_key,
    system_of_record,
    topic_nm,
    'Phone' AS channel,
    CASE
      WHEN OptOut_byPhone = 'False' THEN 'Opt-in'
      WHEN OptOut_byPhone = 'True' THEN 'Opt-out'
  END
    AS event_cd,
    event_dtm
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.aaacom_fact_contact_event_optout_work_source`
  WHERE
    dupe_check=1 )

