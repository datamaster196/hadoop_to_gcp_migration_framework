-------------------------------------------------------------------------------------------------------------------------------------------------
----------- This query depriortises Bounce for the cases where the channel, topic, and event date/time stamp----------------------------
-------------------------------------- are all equivalent but the event codes oppose --------------------------------------------


CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_source2` AS
SELECT
  contact_pref_source.contact_adw_key,
  contact_pref_source.topic_nm,
  contact_pref_source.channel,
  contact_pref_source.event_cd,
  contact_pref_source.event_dtm,
  contact_pref_source.dupe_channel,
  contact_pref_source.prev_dupe_channel
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_source` contact_pref_source
LEFT JOIN (
  SELECT
    contact_adw_key,
    topic_nm,
    channel,
    event_dtm,
    'Bounce' AS event_cd,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_source`
  GROUP BY
    1,
    2,
    3,
    4
  HAVING
    COUNT(*)>1) depriortise_Bounce
ON
  contact_pref_source.contact_adw_key=depriortise_Bounce.contact_adw_key
  AND contact_pref_source.channel=depriortise_Bounce.channel
  AND contact_pref_source.event_dtm=depriortise_Bounce.event_dtm
  AND contact_pref_source.event_cd=depriortise_Bounce.event_cd
WHERE
  depriortise_Bounce.contact_adw_key IS NULL ;


-------------------------------------------------------------------------------------------------------------------------------------------------
----------- This query depriortises Opt-out for the cases where the channel, topic, and event date/time stamp----------------------------
----------- are all equivalent but the event codes oppose and picks the oldest record for the same event_cd---------------------------


CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_source` AS
SELECT
  contact_adw_key,
  topic_nm,
  channel,
  event_cd,
  effective_start_datetime,
  adw_row_hash,
  ROW_NUMBER() OVER(PARTITION BY contact_adw_key, topic_nm, channel ORDER BY effective_start_datetime ASC) AS dupe_channel
FROM (SELECT contact_pref_source.contact_adw_key,
    contact_pref_source.topic_nm,
    contact_pref_source.channel,
    contact_pref_source.event_cd,
    contact_pref_source.event_dtm AS effective_start_datetime,
    TO_BASE64(MD5(CONCAT( ifnull(COALESCE(contact_pref_source.contact_adw_key,
              "-1"),
            ''), '|', ifnull(contact_pref_source.topic_nm,
            ''), '|', ifnull(contact_pref_source.channel,
            ''), '|', ifnull(contact_pref_source.event_cd,
            ''), '|' ))) AS adw_row_hash,
    ifnull((contact_pref_source.dupe_channel- contact_pref_source.prev_dupe_channel),
      0) AS diff_channel 
FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_source2` contact_pref_source
LEFT JOIN (
  SELECT
    contact_adw_key,
    topic_nm,
    channel,
    event_dtm,
    'Opt-out' AS event_cd,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_source2`
      GROUP BY
    1,
    2,
    3,
    4
  HAVING
    COUNT(*)>1) depriortise_Optout
ON
  contact_pref_source.contact_adw_key=depriortise_Optout.contact_adw_key
  AND contact_pref_source.channel=depriortise_Optout.channel
  AND contact_pref_source.event_dtm=depriortise_Optout.event_dtm
  AND contact_pref_source.event_cd=depriortise_Optout.event_cd
WHERE
  depriortise_Optout.contact_adw_key IS NULL )
  WHERE
  diff_channel<>1;