------------------ Incremental Load Prep work ------
-- Clean up work area
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_source`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_contact_split`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed`;
drop table if exists `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_stage`;

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
  WHERE
    CAST(source.adw_lake_insert_datetime AS datetime) > (
    SELECT
      MAX(effective_start_datetime)
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`
    WHERE segment_source = 'SIGMA Add Associate')
   ;
---------------
-- Get contact Info before transposing the record

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
 --  contact_src_m16.mbr_merged_previous_mbr16_id,
 --  substr(source_other.membership_number,4,3) as club_code,
 -- source_other.pred_color_grp,
 --  source_other.predicted_tier,
 --  source_other.SEG2,
 --  source_other.last_upd_dt
--FROM
 -- (select * from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_source` where substr(membership_number,4,3) <> '212' and dupe_check = 1) AS source_other
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
   ROW_NUMBER() OVER(PARTITION BY contact_adw_key, segment_panel,segment_cd ORDER BY last_upd_dt DESC) AS dupe_check
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
----------------------
-- Stage


 CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_stage` AS
  SELECT
    GENERATE_UUID() contact_segmntn_adw_key
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
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed` source
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
     (select max(last_upd_dt) as last_upd_dt from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed`)  source
  CROSS JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation` target
  WHERE
    target.actv_ind='Y'
    AND target.segment_source = 'SIGMA Add Associate'
    ;
-------------------------- End of script ------
