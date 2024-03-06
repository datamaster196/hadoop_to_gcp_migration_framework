  MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_segm_work_stage` b
  ON
    (a.contact_adw_key = b.contact_adw_key and a.segment_source = b.segment_source and a.segment_panel = b.segment_panel
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT (
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
  integrated_update_batch_number   ) VALUES (
  b.contact_segmntn_adw_key       ,
  b.contact_adw_key               ,
  b.segment_source                ,
  b.segment_panel                 ,
  b.segment_cd                    ,
  b.segment_desc                  ,
  b.actual_score_ind              ,
  b.effective_start_datetime      ,
  b.effective_end_datetime        ,
  b.actv_ind                      ,
  b.adw_row_hash                  ,
  b.integrated_insert_datetime         ,
  b.integrated_insert_batch_number   ,
  b.integrated_update_datetime         ,
  b.integrated_update_batch_number
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.actv_ind = b.actv_ind,
    a.integrated_update_datetime = b.integrated_update_datetime,
    a.integrated_update_batch_number = b.integrated_update_batch_number;

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
   (select contact_adw_key ,segment_source ,segment_panel , segment_cd, effective_start_datetime, count(*) as dupe_count
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`
   group by 1, 2, 3, 4,5
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