------------------------------------------------------------------------------------------------------------------------------------------------
----------- This query excludes first Opt-in records for the entity and ranks incremental data on the basis of event_dtm,-----------------------
----------- where hishest rank is given to  the latest record amongst all the records for an entity in incremental and target table ------------


CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_transformed` AS
SELECT
  contact_adw_key,
  topic_nm,
  channel,
  event_cd,
  effective_start_datetime,
  adw_row_hash,
  ROW_NUMBER() OVER(PARTITION BY contact_adw_key, topic_nm, channel ORDER BY effective_start_datetime DESC) AS desc_dupe_channel
FROM (
  SELECT
    transform.*,
    ifnull((dupe_channel- prev_dupe_channel),
      0) AS diff_channel
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_transformed` transform
  LEFT JOIN (
    SELECT
      contact_adw_key,
      topic_nm,
      channel,
      event_cd,
      effective_start_datetime
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_pre_transformed`
    WHERE
      event_cd='Opt-in'
      AND dupe_channel=1) exclude_Optin
  ON
    transform.contact_adw_key=exclude_Optin.contact_adw_key
    AND transform.channel=exclude_Optin.channel
    AND transform.effective_start_datetime=exclude_Optin.effective_start_datetime
    AND transform.event_cd=exclude_Optin.event_cd
  WHERE
    exclude_Optin.contact_adw_key IS NULL )
WHERE
  diff_channel<>1
  AND src='trans';