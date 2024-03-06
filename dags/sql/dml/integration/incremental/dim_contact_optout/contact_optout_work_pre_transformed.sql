-----------------------------------------------------------------------------------------------------
----------- This query ranks channel for each entity for complete dataset----------------------------
 ---------- from incremental as well as exiting data in target tables--------------------------------


CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_transformed` AS
SELECT
  contact_adw_key,
  topic_nm,
  channel,
  event_cd,
  effective_start_datetime,
  adw_row_hash,
  src,
  dupe_channel,
  LAG(dupe_channel) OVER(PARTITION BY contact_adw_key, topic_nm, channel, event_cd ORDER BY channel, effective_start_datetime ASC) AS prev_dupe_channel
FROM (
  SELECT
    contact_adw_key,
    topic_nm,
    channel,
    event_cd,
    effective_start_datetime,
    adw_row_hash,
    src,
    ROW_NUMBER() OVER(PARTITION BY contact_adw_key, topic_nm, channel ORDER BY effective_start_datetime ASC) AS dupe_channel
  FROM (
-- Query1:Picks the active records from dim_Contact_optout for matching incremental records. 
    SELECT
      b.contact_adw_key,
      b.topic_nm,
      b.channel,
      b.event_cd,
      b.effective_start_datetime,
      b.adw_row_hash,
      'dim' AS src
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout` b
    JOIN
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_source` a
    ON
      a.contact_adw_key=b.contact_adw_key
      AND a.channel=b.channel
      AND a.topic_nm=b.topic_nm
    WHERE
      b.actv_ind='Y'
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
    UNION ALL
-- Query2: Pulls all the incremental records.
    SELECT
      a.contact_adw_key,
      a.topic_nm,
      a.channel,
      a.event_cd,
      a.effective_start_datetime,
      a.adw_row_hash,
      'trans' AS src
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_source` a ) )
