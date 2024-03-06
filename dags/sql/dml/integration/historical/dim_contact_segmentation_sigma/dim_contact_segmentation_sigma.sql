INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`
  (
  contact_segmntn_adw_key       ,
  contact_adw_key               ,
  segment_source                ,
  segment_panel                 ,
  segment_cd                    ,
  segment_desc                  ,
  actual_score_ind              ,
  effective_start_datetime      ,
  effective_end_datetime        ,
  actv_ind                      ,
  adw_row_hash                  ,
  integrated_insert_datetime         ,
  integrated_insert_batch_number   ,
  integrated_update_datetime         ,
  integrated_update_batch_number )
SELECT
  pk.contact_segmntn_adw_key           ,
  source.contact_adw_key               ,
  source.segment_source                ,
  source.segment_panel                 ,
  source.segment_cd                    ,
  source.segment_desc                  ,
  source.actual_score_ind              ,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed` source ON (
  source.contact_adw_key = hist_type_2.contact_adw_key
  AND source.segment_source = hist_type_2.segment_source
  AND source.segment_panel = hist_type_2.segment_panel
  AND source.segment_cd = hist_type_2.segment_cd
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS contact_segmntn_adw_key,
    contact_adw_key,
    segment_source,
    segment_panel,
    segment_cd
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_cont_segm_sig_work_transformed`
  GROUP BY
    contact_adw_key,
    segment_source,
    segment_panel,
    segment_cd ) pk
ON (      source.contact_adw_key = pk.contact_adw_key
      AND source.segment_source = pk.segment_source
      AND source.segment_panel = pk.segment_panel
      AND source.segment_cd  = pk.segment_cd );

      --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for address_adw_key

SELECT
     count(target.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`) source_FK_1
                          where target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw_pii.dim_contact_info. FK Column: contact_adw_key'));

----------------------------------------------------------------------------------------------
-- Duplicate Checks

 select count(1)
   from
   (select contact_adw_key ,segment_source ,segment_panel ,segment_cd , effective_start_datetime, count(*) as dupe_count
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`
   group by 1, 2,3,4, 5
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_contact_segmentation'  ) );

 ---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a.contact_segmntn_adw_key ) from
 (select contact_segmntn_adw_key, contact_adw_key ,segment_source ,segment_panel , segment_cd, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`) a
 join
 (select contact_segmntn_adw_key, contact_adw_key ,segment_source ,segment_panel, segment_cd, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`) b
 on a.contact_segmntn_adw_key=b.contact_segmntn_adw_key
    and a.contact_adw_key  = b.contact_adw_key
    and a.segment_source  = b.segment_source
    and a.segment_panel  = b.segment_panel
    and a.segment_cd = b.segment_cd
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.contact_segmntn_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_contact_segmentation' ));