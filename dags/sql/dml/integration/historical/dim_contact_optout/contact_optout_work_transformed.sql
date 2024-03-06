--------------------------------------------------------------------------------------------------------------
----------- This query excludes first Opt-in records for the entity---------------------------


CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_transformed` AS

  SELECT
    transform.*
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_source` transform
  LEFT JOIN (
    SELECT
      contact_adw_key,
      topic_nm,
      channel,
      event_cd,
      effective_start_datetime
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_source`
    WHERE
      event_cd='Opt-in'
      AND dupe_channel=1) exclude_Optin
  ON
    transform.contact_adw_key=exclude_Optin.contact_adw_key
    AND transform.channel=exclude_Optin.channel
    AND transform.effective_start_datetime=exclude_Optin.effective_start_datetime
    AND transform.event_cd=exclude_Optin.event_cd
  WHERE
    exclude_Optin.contact_adw_key IS NULL 
;
 