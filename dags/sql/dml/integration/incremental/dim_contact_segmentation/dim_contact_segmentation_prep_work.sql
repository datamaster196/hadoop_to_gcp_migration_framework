------------------ Incremental Load Prep work ------

-- Clean up work area
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_source`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact_split`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_transformed`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_stage`;

-- Source
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_source` AS
SELECT
   source.membership_number,
   source.pred_color_grp,
   source.predicted_tier,
   source.SEG2,
   source.adw_lake_insert_datetime as last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY source.membership_number ORDER BY source.adw_lake_insert_datetime DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.csaa.csaa_panel` AS source
  WHERE
    CAST(source.adw_lake_insert_datetime AS datetime) > (
    SELECT
      MAX(effective_start_datetime)
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`)
   ;
---------------
-- Get contact Info before transposing the record

CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact` AS
SELECT
   contact_src.contact_adw_key,
   source_212.membership_number,
   null as member16_id,
   substr(source_212.membership_number,4,3) as club_code,
   source_212.pred_color_grp,
   source_212.predicted_tier,
   source_212.SEG2,
   source_212.last_upd_dt
FROM
  (select * from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_source` where substr(membership_number,4,3) = '212' and dupe_check = 1)  AS source_212
  INNER JOIN
    (select contact_adw_key, mbrs_id, mbr_assoc_id, mbr_merged_previous_mbr16_id from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` where actv_ind = 'Y' and contact_adw_key <> '-1') AS contact_src
      on substr(source_212.membership_number,7,7) = contact_src.mbrs_id and substr(source_212.membership_number,14,2) = contact_src.mbr_assoc_id
 UNION ALL
 SELECT
   contact_src_m16.contact_adw_key,
   source_other.membership_number,
   contact_src_m16.mbr_merged_previous_mbr16_id,
   substr(source_other.membership_number,4,3) as club_code,
   source_other.pred_color_grp,
   source_other.predicted_tier,
   source_other.SEG2,
   source_other.last_upd_dt
FROM
  (select * from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_source` where substr(membership_number,4,3) <> '212' and dupe_check = 1) AS source_other
  INNER JOIN
    (select contact_adw_key, mbrs_id, mbr_assoc_id, mbr_merged_previous_mbr16_id from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` where actv_ind = 'Y' and contact_adw_key <> '-1') AS contact_src_m16
      on source_other.membership_number = contact_src_m16.mbr_merged_previous_mbr16_id
 ;

---------------
-- Splitting of source data

CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact_split` AS
select
   contact_adw_key,
   segment_panel,
   segment_cd,
   last_upd_dt,
   ROW_NUMBER() OVER(PARTITION BY contact_adw_key, segment_panel ORDER BY last_upd_dt DESC) AS dupe_check
from
(
  SELECT
     contact_adw_key,
     'Predicted Color Group' as segment_panel,
     pred_color_grp as segment_cd,
     last_upd_dt
  from
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact` where coalesce(trim(pred_color_grp), '') <> ''
  union all
  SELECT
     contact_adw_key,
     'Predicted Tier' as segment_panel,
     predicted_tier as segment_cd,
     last_upd_dt
  from
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact` where coalesce(trim(predicted_tier), '') <> ''
  union all
  SELECT
     contact_adw_key,
     'Signature Series Tier Proxy Model' as segment_panel,
     SEG2 as segment_cd,
     last_upd_dt
  from
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact` where coalesce(trim(SEG2), '') <> ''
) csaa_segm
;
---------------
-- Transformations


CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_transformed` AS
SELECT
  csaa_segm.contact_adw_key
 ,'CSAA' as segment_source
 ,csaa_segm.segment_panel
 ,csaa_segm.segment_cd
 ,case  -- Panel = Predicted Color Group
       when csaa_segm.segment_panel = 'Predicted Color Group' then coalesce(csaa_segm.segment_cd,'')

       -- Panel = Predicted Tier
       when csaa_segm.segment_panel = 'Predicted Tier' and coalesce(trim(segment_cd),'') = 'A' then 'Aspiring'
       when csaa_segm.segment_panel = 'Predicted Tier' and coalesce(trim(segment_cd),'') = 'C' then 'Competitive'
	   when csaa_segm.segment_panel = 'Predicted Tier' and coalesce(trim(segment_cd),'') = 'T' then 'Target'

       -- Panel = Signature Series Tier Proxy Model
       when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '1' then 'Relationship and Service Traditionalists'
       when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '2' then 'Disengaged Non-Shoppers'
	   when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '3' then 'Omni-Channel Assurance Seekers'
	   when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '4' then 'Price-Motivated Independents'
	   else ''
  end as segment_desc
 ,'Y' as actual_score_ind
 ,cast (csaa_segm.last_upd_dt AS datetime) AS last_upd_dt,
    TO_BASE64(MD5(CONCAT(ifnull(csaa_segm.contact_adw_key,
            ''),'|',ifnull('CSAA',
	        ''),'|',ifnull(coalesce(trim(csaa_segm.segment_panel),''),
	        ''),'|',ifnull(coalesce(trim(case  -- Panel = Predicted Color Group
       when csaa_segm.segment_panel = 'Predicted Color Group' then coalesce(csaa_segm.segment_cd,'')

       -- Panel = Predicted Tier
       when csaa_segm.segment_panel = 'Predicted Tier' and coalesce(trim(csaa_segm.segment_cd),'') = 'A' then 'Aspiring'
       when csaa_segm.segment_panel = 'Predicted Tier' and coalesce(trim(csaa_segm.segment_cd),'') = 'C' then 'Competitive'
	   when csaa_segm.segment_panel = 'Predicted Tier' and coalesce(trim(csaa_segm.segment_cd),'') = 'T' then 'Target'

       -- Panel = Signature Series Tier Proxy Model
       when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '1' then 'Relationship and Service Traditionalists'
       when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '2' then 'Disengaged Non-Shoppers'
	   when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '3' then 'Omni-Channel Assurance Seekers'
	   when csaa_segm.segment_panel = 'Signature Series Tier Proxy Model' and coalesce(trim(csaa_segm.segment_cd),'') = '4' then 'Price-Motivated Independents'
	   else ''
  end),''),
	        ''),'|',ifnull(coalesce(trim('1'),''),
            '') ))) AS adw_row_hash
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact_split` AS csaa_segm
  WHERE dupe_check = 1
;
----------------------
-- Stage


 CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_stage` AS
  SELECT
    COALESCE(target.contact_segmntn_adw_key,
      GENERATE_UUID()) contact_segmntn_adw_key
   ,source.contact_adw_key
   ,source.segment_source
   ,source.segment_panel
   ,source.segment_cd
   ,source.segment_desc
   ,source.actual_score_ind
   ,source.last_upd_dt as effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS actv_ind,
    source.adw_row_hash,
    CURRENT_DATETIME() integrated_insert_datetime,
    {{ dag_run.id }} integrated_insert_batch_number,
    CURRENT_DATETIME() integrated_update_datetime,
    {{ dag_run.id }} integrated_update_batch_number
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_transformed` source
  LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation` target
  ON
    (
	  source.contact_adw_key=target.contact_adw_key
	  AND
	  source.segment_source=target.segment_source
	  AND
	  source.segment_panel=target.segment_panel
      AND target.actv_ind='Y')
  WHERE
    target.contact_segmntn_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
  UNION ALL
  SELECT
    target.contact_segmntn_adw_key
   ,target.contact_adw_key
   ,target.segment_source
   ,target.segment_panel
   ,target.segment_cd
   ,target.segment_desc
   ,target.actual_score_ind
   ,target.effective_start_datetime
   ,DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 second) AS effective_end_datetime
   ,'N' AS actv_ind
   ,target.adw_row_hash
   ,target.integrated_insert_datetime
   ,target.integrated_insert_batch_number
   ,CURRENT_DATETIME()
   ,target.integrated_update_batch_number
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_transformed` source
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation` target
  ON
    ( source.contact_adw_key=target.contact_adw_key
	  AND
	  source.segment_source=target.segment_source
	  AND
	  source.segment_panel=target.segment_panel
      AND target.actv_ind='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash
    ;
-------------------------- End of script ------
