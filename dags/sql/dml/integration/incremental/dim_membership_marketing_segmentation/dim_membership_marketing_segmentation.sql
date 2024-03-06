MERGE INTO
     `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_mbrs_marketing_segmntn` a
   USING
     `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_stage` b
   ON
     (a.mbrs_marketing_segmntn_adw_key = b.mbrs_marketing_segmntn_adw_key
	   AND a.mbr_expiration_dt = b.member_expiration_dt
	   AND a.segment_nm = b.segment_nm
     AND a.segmntn_panel_cd = b.segmntn_panel_cd
     AND a.effective_start_datetime = b.effective_start_datetime)
     WHEN NOT MATCHED THEN INSERT (
     mbrs_marketing_segmntn_adw_key
     ,mbrs_adw_key
     ,mbr_expiration_dt
     ,segmntn_test_group_cd
     ,segmntn_panel_cd
     ,segmntn_control_panel_ind
     ,segmntn_comm_cd
     ,segment_nm
     ,adw_row_hash
     ,effective_start_datetime
     ,effective_end_datetime
     ,actv_ind
     ,integrate_insert_datetime
     ,integrate_insert_batch_number
     ,integrate_update_datetime
     ,integrate_update_batch_number
    ) VALUES (
      b.mbrs_marketing_segmntn_adw_key
     ,b.mbrs_adw_key
     ,b.member_expiration_dt
     ,b.segmntn_test_group_cd
     ,b.segmntn_panel_cd
     ,b.segmntn_control_panel_ind
     ,b.segmntn_comm_cd
     ,b.segment_nm
     ,b.adw_row_hash
     ,b.effective_start_datetime
     ,b.effective_end_datetime
     ,b.actv_ind
     ,b.integrate_insert_datetime
     ,b.integrate_insert_batch_number
     ,b.integrate_update_datetime
     ,b.integrate_update_batch_number
   )
   WHEN MATCHED
   THEN
 UPDATE
 SET
   a.effective_end_datetime = b.effective_end_datetime,
   a.actv_ind = b.actv_ind,
   a.integrate_update_datetime = b.integrate_update_datetime,
   a.integrate_update_batch_number = b.integrate_update_batch_number;

--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for dim_membership_marketing_segmentation
SELECT
         count(target.mbrs_adw_key ) AS mbrs_adw_key
FROM
         (select distinct mbrs_adw_key
         from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_mbrs_marketing_segmntn`)  target
         where not exists (select 1
                          from (select distinct mbrs_adw_key
                                from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`) source_FK_1
                                where target.mbrs_adw_key = source_FK_1.mbrs_adw_key)
HAVING
IF((mbrs_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_mbrs_marketing_segmntn. FK Column: mbrs_adw_key'));

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
    (select mbrs_adw_key, mbr_expiration_dt, segment_nm, segmntn_panel_cd, effective_start_datetime, count(*) as dupe_count
     from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_mbrs_marketing_segmntn`
     group by 1, 2, 3, 4, 5
     having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_mbrs_marketing_segmntn'  ) );


---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.mbrs_marketing_segmntn_adw_key )
from
  (select mbrs_marketing_segmntn_adw_key , effective_start_datetime, effective_end_datetime from
   `{{var.value.INTEGRATION_PROJECT}}.adw.dim_mbrs_marketing_segmntn`) a
join
  (select mbrs_marketing_segmntn_adw_key, effective_start_datetime, effective_end_datetime from
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_mbrs_marketing_segmntn`) b
on a.mbrs_marketing_segmntn_adw_key=b.mbrs_marketing_segmntn_adw_key
       and a.effective_start_datetime  <= b.effective_end_datetime
       and b.effective_start_datetime  <= a.effective_end_datetime
       and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.mbrs_marketing_segmntn_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_mbrs_marketing_segmntn' ));