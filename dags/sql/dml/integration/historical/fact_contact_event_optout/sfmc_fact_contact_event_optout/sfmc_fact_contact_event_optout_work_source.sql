CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.sfmc_fact_contact_event_optout_work_source` AS
  ------------------------------------------------------------------------
  --------------------------------Source SFMC------------------------------

SELECT
  contact_adw_key,
  source_system_key,
  system_of_record,
  topic_nm,
  channel,
  event_cd,
  event_dtm
 FROM 
  (
  SELECT contact_adw_key, source_system_key, system_of_record, topic_nm, channel, event_cd, event_dtm,
  ROW_NUMBER() OVER(PARTITION BY contact_adw_key, source_system_key, system_of_record, topic_nm, channel, event_cd, event_dtm ORDER BY NULL ASC) AS dupe_check
 FROM 
  (
   SELECT
     coalesce(contact_N_eff.contact_adw_key, contact_Y.contact_adw_key,'-1') as contact_adw_key,
     SFMC.source_system_key, SFMC.system_of_record, SFMC.topic_nm, SFMC.channel, SFMC.event_cd, SFMC.event_dtm
     FROM
   (
     SELECT distinct
       CONCAT(subscriberkey,listid) AS source_system_key,
       'SFMC' AS system_of_record,
       listname AS topic_nm,
       'All' AS channel,
       'Opt-out' AS event_cd,
       CASE
         WHEN trim(dateunsubscribed) ='' THEN SAFE_CAST(createddate AS string)
         WHEN dateunsubscribed IS NULL THEN SAFE_CAST(createddate AS string)
       ELSE
       SAFE_CAST(dateunsubscribed AS string)
     END
       AS event_dtm,
       'listsubscribers' AS src
     FROM
       `{{ var.value.INGESTION_PROJECT }}.sfmc.listsubscribers` list
     WHERE
       listid IN ('572','573','574','576','577','578','579','580','581','582','583','584','585')
     UNION DISTINCT
     SELECT distinct
       subscriberkey AS source_system_key,
       'SFMC' AS system_of_record,
       '' AS topic_nm,
       'Email' AS channel,
       CASE
         WHEN status IN ('held', 'bounced') THEN 'Bounce'
         WHEN status='unsubscribed' THEN 'Opt-out'
         WHEN status ='active' THEN 'Opt-in'
     END
       AS event_cd,
       CASE
         WHEN status IN ('held', 'bounced') AND trim(dateundeliverable)!='' THEN SAFE_CAST(dateundeliverable AS string)
         WHEN status='unsubscribed'  AND  trim(dateunsubscribed)!=''    THEN SAFE_CAST(dateunsubscribed AS string)
       ELSE
       SAFE_CAST(datejoined AS string)
     END
       AS event_dtm,
       'subscribers' AS src
     FROM
       `{{ var.value.INGESTION_PROJECT }}.sfmc.subscribers` sub
     UNION DISTINCT
     SELECT distinct
       unsub.subscriberkey AS source_system_key,
       'SFMC' AS system_of_record,
       '' AS topic_nm,
       'Email' AS channel,
       'Opt-out' AS event_cd,
       CASE
         WHEN trim(eventdate) ='' THEN SAFE_CAST(unsub.adw_lake_insert_datetime AS string)
         WHEN eventdate IS NULL THEN SAFE_CAST(unsub.adw_lake_insert_datetime AS string)
       ELSE
       SAFE_CAST(eventdate AS string)
     END
       AS event_dtm,
       'unsubscribe' AS src
     FROM
       `{{ var.value.INGESTION_PROJECT }}.sfmc.unsubscribe` unsub
     LEFT JOIN
       `{{ var.value.INGESTION_PROJECT }}.sfmc.subscribers` sub
     ON
       unsub.subscriberkey=sub.subscriberkey
       AND sub.status='unsubscribed'
     WHERE
       sub.subscriberkey IS NULL
     UNION DISTINCT
     SELECT distinct
       bounce.subscriberkey AS source_system_key,
       'SFMC' AS system_of_record,
       '' AS topic_nm,
       'Email' AS channel,
       'Bounce' AS event_cd,
       CASE
         WHEN trim(eventdate)='' THEN SAFE_CAST(bounce.adw_lake_insert_datetime AS string)
         WHEN eventdate IS NULL THEN SAFE_CAST(bounce.adw_lake_insert_datetime AS string)
       ELSE
       SAFE_CAST(eventdate AS string)
     END
       AS event_dtm,
       'bounce' AS src
     FROM
       `{{ var.value.INGESTION_PROJECT }}.sfmc.bounce` bounce
     LEFT JOIN
       `{{ var.value.INGESTION_PROJECT }}.sfmc.subscribers` sub
     ON
       bounce.subscriberkey=sub.subscriberkey
       AND sub.status IN ('held',
         'bounced')
     WHERE
       sub.subscriberkey IS NULL 
   ) SFMC
   LEFT JOIN `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` contact_N_eff
   on contact_N_eff.key_typ_nm LIKE 'customer_id%'
      and SFMC.source_system_key = contact_N_eff.source_1_key and safe_cast(SFMC.event_dtm as datetime) between contact_N_eff.effective_start_datetime and contact_N_eff.effective_end_datetime
   LEFT JOIN (select 
	              contact_adw_key, 
	              source_1_key, 
	              row_number() over(partition by source_1_key order by effective_end_datetime desc) rn 
	            from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` 
	            where key_typ_nm like 'customer_id%' 
             ) contact_Y
   on SFMC.source_system_key = contact_Y.source_1_key and contact_Y.rn=1
	) contact	 
 where contact.contact_adw_key <> '-1'
) source
where dupe_check = 1