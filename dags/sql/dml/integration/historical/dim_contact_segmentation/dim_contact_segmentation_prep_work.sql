------------------ Historical Load Prep work ------
-- Clean up work area
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_source`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_contact_split`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_transformed`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_type_2_hist`;

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
  ;
---------------
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
   ROW_NUMBER() OVER(PARTITION BY contact_adw_key, segment_panel, last_upd_dt ORDER BY NULL) AS dupe_check
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
-----------------

CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_type_2_hist` AS
WITH
  contact_segmentation_hist AS (
  SELECT
    contact_adw_key
   ,segment_source
   ,segment_panel
   ,segment_cd
   ,segment_desc
   ,actual_score_ind
   ,last_upd_dt AS effective_start_datetime
   ,adw_row_hash
   ,LAG(adw_row_hash) OVER (PARTITION BY contact_adw_key,segment_source,segment_panel ORDER BY last_upd_dt) AS prev_row_hash
   ,coalesce(LEAD(last_upd_dt) OVER (PARTITION BY contact_adw_key,segment_source,segment_panel ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_transformed`
  ), set_grouping_column AS (
  SELECT
	 contact_adw_key
	,segment_source
	,segment_panel
    ,adw_row_hash
    ,CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag
    ,effective_start_datetime
    ,next_record_datetime
  FROM
    contact_segmentation_hist
  ), set_groups AS (
  SELECT
	 contact_adw_key
	,segment_source
	,segment_panel
    ,adw_row_hash
    ,SUM(new_record_tag) OVER (PARTITION BY contact_adw_key,segment_source,segment_panel ORDER BY effective_start_datetime) AS grouping_column
    ,effective_start_datetime
    ,next_record_datetime
  FROM
    set_grouping_column
  ), deduped AS (
  SELECT
	 contact_adw_key
	,segment_source
	,segment_panel
    ,adw_row_hash
    ,grouping_column
    ,MIN(effective_start_datetime) AS effective_start_datetime
    ,MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
	 contact_adw_key
	,segment_source
	,segment_panel
    ,adw_row_hash
    ,grouping_column
  )
SELECT
  contact_adw_key
 ,segment_source
 ,segment_panel
 ,adw_row_hash
 ,effective_start_datetime
 ,CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped;
-------------------------- End of script ------