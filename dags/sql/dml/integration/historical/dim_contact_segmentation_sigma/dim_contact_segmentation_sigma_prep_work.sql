------------------ Historical Load Prep work ------
-- Clean up work area
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_source`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact_split`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_type_2_hist`;

-- Source
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_source` AS
SELECT
   source.membershipid,
   source.panel_code,
   source.promo_code,
   source.adw_lake_insert_datetime as last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY source.membershipid, source.panel_code, source.promo_code, source.adw_lake_insert_datetime ORDER BY source.adw_lake_insert_datetime DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.sigma.aaa_ca_em_add_assoc` AS source
  ;
---------------
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact` AS
SELECT
   contact_src.contact_adw_key,
   source_212.membershipid,
   null as member16_id,
   source_212.panel_code,
   source_212.promo_code,
   source_212.last_upd_dt
FROM
  (select * from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_source` where dupe_check = 1)  AS source_212
  INNER JOIN
    (select contact_adw_key, mbrs_id, mbr_assoc_id, mbr_merged_previous_mbr16_id from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` where actv_ind = 'Y' and contact_adw_key <> '-1' and prim_mbr_cd = 'P') AS contact_src
      on substr(source_212.membershipid,1,7) = contact_src.mbrs_id
 --UNION ALL
 --SELECT
 --  contact_src_m16.contact_adw_key,
 --  source_other.membership_number,
  -- contact_src_m16.mbr_merged_previous_mbr16_id,
   --substr(source_other.membership_number,4,3) as club_code,
 --  source_other.pred_color_grp,
 --  source_other.predicted_tier,
 --  source_other.SEG2,
 --  source_other.last_upd_dt
--FROM
 -- (select * from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_source` where  dupe_check = 1) AS source_other
  --INNER JOIN
   -- (select contact_adw_key, mbrs_id, mbr_assoc_id, mbr_merged_previous_mbr16_id from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` where actv_ind = 'Y' and contact_adw_key <> '-1') AS contact_src_m16
    --  on source_other.membership_number = contact_src_m16.mbr_merged_previous_mbr16_id
 ;
---------------
-- Splitting of source data

CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact_split` AS
select
   contact_adw_key,
   segment_panel,
   segment_cd,
   last_upd_dt,
   ROW_NUMBER() OVER(PARTITION BY contact_adw_key, segment_panel,segment_cd, last_upd_dt ORDER BY NULL) AS dupe_check
from
(
  SELECT
     contact_adw_key,
     'Panel Code' as segment_panel,
     panel_code as segment_cd,
     last_upd_dt
  from
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact` where coalesce(trim(panel_code), '') <> ''
  union all
  SELECT
     contact_adw_key,
     'Promo Code' as segment_panel,
     promo_code as segment_cd,
     last_upd_dt
  from
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact` where coalesce(trim(promo_code), '') <> ''
) sig_segm
;
---------------
-- Transformations


CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed` AS
SELECT
  sig_segm.contact_adw_key
 ,'SIGMA Add Associate' as segment_source
 ,sig_segm.segment_panel
 ,sig_segm.segment_cd
 ,'' as segment_desc
 ,'Y' as actual_score_ind
 ,cast (sig_segm.last_upd_dt AS datetime) AS last_upd_dt,
    TO_BASE64(MD5(CONCAT(ifnull(cast (sig_segm.last_upd_dt AS string),
            ''),'|',ifnull('SIGMA Add Associate',
	        '') ))) AS adw_row_hash
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact_split` AS sig_segm
  WHERE dupe_check = 1
;
-----------------

CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_type_2_hist` AS
WITH
  contact_segmentation_grouping AS (
  SELECT
   last_upd_dt AS effective_start_datetime
   ,adw_row_hash
   ,LAG(adw_row_hash) OVER (PARTITION BY adw_row_hash ORDER BY last_upd_dt) AS prev_row_hash
   ,coalesce(LEAD(last_upd_dt) OVER (ORDER BY last_upd_dt),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed`
    group by last_upd_dt, adw_row_hash
  ), set_effective_end AS (
   SELECT
	 a.contact_adw_key
	,a.segment_source
	,a.segment_panel
	,a.segment_cd
    ,a.adw_row_hash
    ,b.effective_start_datetime
    ,b.next_record_datetime as effective_end_datetime
    FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed` a join
    contact_segmentation_grouping b on a.adw_row_hash = b.adw_row_hash
  )SELECT
  contact_adw_key
 ,segment_source
 ,segment_panel
 ,segment_cd
 ,adw_row_hash
 ,effective_start_datetime
 ,CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  set_effective_end;
-------------------------- End of script ------