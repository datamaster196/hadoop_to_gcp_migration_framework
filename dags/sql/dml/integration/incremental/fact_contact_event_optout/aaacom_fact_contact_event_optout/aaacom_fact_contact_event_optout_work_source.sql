CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.aaacom_fact_contact_event_optout_work_source` AS
  ------------------------------------------------------------------------
  --------------------------------Source AAA------------------------------

SELECT
    contact_adw_key,
  source_system_key,
  system_of_record,
  topic_nm,
  OptOut_byEmail,
  OptOut_byMail,
  OptOut_byPhone,
  event_dtm,
  submitDate,
  dupe_check
FROM (
  SELECT
    mem.contact_adw_key,
    SUBSTR(aaacom.membershipID,-16) AS source_system_key,
    'AAACOM' AS system_of_record,
    '' AS topic_nm,
    OptOut_byEmail,
    OptOut_byMail,
    OptOut_byPhone,
    CASE
      WHEN submitDate ='' THEN SAFE_CAST(adw_lake_insert_datetime AS string)
      WHEN submitDate IS NULL THEN SAFE_CAST(adw_lake_insert_datetime AS string)
    ELSE
    SAFE_CAST(submitDate AS string)
  END
    AS event_dtm,
    submitDate,
    ROW_NUMBER() OVER(PARTITION BY mem.contact_adw_key, OptOut_byEmail, OptOut_byMail, OptOut_byPhone, submitDate ORDER BY NULL) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.aaacom.aaacomoptout` aaacom
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` mem
  ON
    SUBSTR(aaacom.membershipID,7,7) = mem.mbrs_id                    --7 digit member identifier
    AND SUBSTR(aaacom.membershipID,14,2) = mem.mbr_assoc_id
    AND actv_ind='Y'
  WHERE
    SUBSTR(aaacom.membershipID,4,3) = '212'

  UNION ALL
  SELECT
    mem.contact_adw_key,
    SUBSTR(aaacom.membershipID,-16) AS source_system_key,
    'AAACOM' AS system_of_record,
    '' AS topic_nm,
    OptOut_byEmail,
    OptOut_byMail,
    OptOut_byPhone,
    CASE
      WHEN submitDate ='' THEN SAFE_CAST(adw_lake_insert_datetime AS string)
      WHEN submitDate IS NULL THEN SAFE_CAST(adw_lake_insert_datetime AS string)
    ELSE
    SAFE_CAST(submitDate AS string)
  END
    AS event_dtm,
    submitDate,
    ROW_NUMBER() OVER(PARTITION BY mem.contact_adw_key, OptOut_byEmail, OptOut_byMail, OptOut_byPhone, submitDate ORDER BY NULL) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.aaacom.aaacomoptout` aaacom
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` mem
  ON
    aaacom.membershipID = mem.mbr_merged_previous_mbr16_id                    --16 digit previous member identifier before the club merger
    AND actv_ind='Y'
  WHERE
    SUBSTR(aaacom.membershipID,4,3) <> '212' )
WHERE
  dupe_check=1
  AND
  PARSE_DATETIME('%m/%d/%Y %r',event_dtm ) > (
  SELECT
    MAX(event_dtm)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_contact_event_optout`
  WHERE
    system_of_record='AAACOM' )

