  MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_work_stage` b
  ON
    (a.mbrs_adw_key = b.mbrs_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT (
    mbrs_adw_key,
    address_adw_key,
    phone_adw_key,
    product_adw_key,
    aca_office_adw_key,
    mbrs_source_system_key,
    mbrs_id ,
    mbrs_status_cd,
    mbrs_billing_cd,
    mbrs_status_dtm,
    mbrs_cancel_dt,
    mbrs_billing_category_cd,
    mbrs_dues_cost_amt,
    mbrs_dues_adj_amt,
    mbrs_future_cancel_ind,
    mbrs_future_cancel_dt,
    mbrs_out_of_territory_cd,
    mbrs_address_change_dtm,
    mbrs_cdx_export_update_dtm,
    mbrs_tier_key,
    mbrs_temporary_address_ind,
    previous_club_mbrs_id ,
    previous_club_cd,
    mbrs_transfer_club_cd,
    mbrs_transfer_dtm,
    mbrs_merged_previous_club_cd,
    mbrs_mbrs_merged_previous_id ,
    mbrs_mbrs_merged_previous_key,
    mbrs_dn_solicit_ind,
    mbrs_bad_address_ind,
    mbrs_dn_send_pubs_ind,
    mbrs_market_tracking_cd,
    mbrs_salvage_ind,
    mbrs_salvaged_ind,
    mbrs_dn_salvage_ind,
    mbrs_group_cd,
    mbrs_typ_cd,
    mbrs_ebill_ind,
    mbrs_marketing_segmntn_cd,
    mbrs_promo_cd,
    mbrs_phone_typ_cd,
    mbrs_ers_abuser_ind,
    mbrs_shadow_mcycle_ind,
    mbrs_student_ind,
    mbrs_address_ncoa_update_ind,
    mbrs_assigned_retention_ind,
    mbrs_mbrs_no_promotion_ind,
    mbrs_dn_offer_plus_ind,
    mbrs_aaawld_online_ed_ind,
    mbrs_shadowrv_coverage_ind,
    mbrs_heavy_ers_ind,
    mbrs_purge_ind,
    mbrs_unapplied_amt,
    mbrs_advance_payment_amt,
    mbrs_balance_amt,
    payment_plan_future_cancel_dt,
    payment_plan_future_cancel_ind,
    switch_prim_ind,
    supress_bill_ind,
    offer_end_dt,
    old_payment_plan_ind,
    carry_over_amt,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number ) VALUES (
    b.mbrs_adw_key,
    b.address_adw_key,
    b.phone_adw_key,
    b.product_adw_key,
    b.aca_office_adw_key,
    b.mbrs_source_system_key,
    b.mbrs_id ,
    b.mbrs_status_cd,
    b.mbrs_billing_cd,
    b.mbrs_status_dtm,
    b.mbrs_cancel_dt,
    b.mbrs_billing_category_cd,
    b.mbrs_dues_cost_amt,
    b.mbrs_dues_adj_amt,
    b.mbrs_future_cancel_ind,
    b.mbrs_future_cancel_dt,
    b.mbrs_out_of_territory_cd,
    b.mbrs_address_change_dtm,
    b.mbrs_cdx_export_update_dtm,
    b.mbrs_tier_key,
    b.mbrs_temporary_address_ind,
    b.previous_club_mbrs_id ,
    b.previous_club_cd,
    b.mbrs_transfer_club_cd,
    b.mbrs_transfer_dtm,
    b.mbrs_merged_previous_club_cd,
    b.mbrs_mbrs_merged_previous_id ,
    b.mbrs_mbrs_merged_previous_key,
    b.mbrs_dn_solicit_ind,
    b.mbrs_bad_address_ind,
    b.mbrs_dn_send_pubs_ind,
    b.mbrs_market_tracking_cd,
    b.mbrs_salvage_ind,
    b.mbrs_salvaged_ind,
    b.mbrs_dn_salvage_ind,
    b.mbrs_group_cd,
    b.mbrs_typ_cd,
    b.mbrs_ebill_ind,
    b.mbrs_marketing_segmntn_cd,
    b.mbrs_promo_cd,
    b.mbrs_phone_typ_cd,
    b.mbrs_ers_abuser_ind,
    b.mbrs_shadow_mcycle_ind,
    b.mbrs_student_ind,
    b.mbrs_address_ncoa_update_ind,
    b.mbrs_assigned_retention_ind,
    b.mbrs_mbrs_no_promotion_ind,
    b.mbrs_dn_offer_plus_ind,
    b.mbrs_aaawld_online_ed_ind,
    b.mbrs_shadowrv_coverage_ind,
    b.mbrs_heavy_ers_ind,
    b.mbrs_purge_ind,
    b.mbrs_unapplied_amt,
    b.mbrs_advance_payment_amt,
    b.mbrs_balance_amt,
    b.payment_plan_future_cancel_dt,
    b.payment_plan_future_cancel_ind,
    b.switch_prim_ind,
    b.supress_bill_ind,
    b.offer_end_dt,
    b.old_payment_plan_ind,
    b.carry_over_amt,
    b.effective_start_datetime,
    b.effective_end_datetime,
    b.actv_ind,
    b.adw_row_hash,
    b.integrate_insert_datetime,
    b.integrate_insert_batch_number,
    b.integrate_update_datetime,
    b.integrate_update_batch_number
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.actv_ind = b.actv_ind,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number;

  UPDATE  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` m
  SET m.mbrs_balance_amt = CAST(ms.amount_due as numeric)
  FROM `{{ var.value.INGESTION_PROJECT }}.mzp.membership_balance` ms
  WHERE m.mbrs_source_system_key = CAST(ms.membership_ky as INT64)
  and m.actv_ind = 'Y'
  and m.mbrs_balance_amt <> CAST(ms.amount_due as numeric);
      --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for address_adw_key

SELECT
     count(target.address_adw_key) AS address_adw_key_count
 FROM
     (select distinct address_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`)  target
       where not exists (select 1
                      from (select distinct address_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_address`) source_FK_1
                          where target.address_adw_key = source_FK_1.address_adw_key)
HAVING
 IF((address_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership. FK Column: address_adw_key'));
 
  -- Orphaned foreign key check for phone_adw_key
 SELECT
     count(target.phone_adw_key ) AS phone_adw_key_count
 FROM
     (select distinct phone_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`)  target
       where not exists (select 1
                      from (select distinct phone_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_phone`) source_FK_2
                          where target.phone_adw_key = source_FK_2.phone_adw_key)
HAVING
 IF((phone_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership. FK Column: phone_adw_key'));

  -- Orphaned foreign key check for product_adw_key
 SELECT
     count(target.product_adw_key ) AS product_adw_key_count
 FROM
     (select distinct product_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`)  target
       where not exists (select 1
                      from (select distinct product_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) source_FK_3
                          where target.product_adw_key = source_FK_3.product_adw_key)
HAVING
 IF((product_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership. FK Column: product_adw_key'));

  -- Orphaned foreign key check for aca_office_adw_key
 SELECT
     count(target.aca_office_adw_key ) AS aca_office_adw_key_count
 FROM
     (select distinct aca_office_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`)  target
       where not exists (select 1
                      from (select distinct aca_office_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office`) source_FK_4
                          where target.aca_office_adw_key = source_FK_4.aca_office_adw_key)
HAVING
 IF((aca_office_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership. FK Column: aca_office_adw_key'));

----------------------------------------------------------------------------------------------
-- Duplicate Checks

 select count(1)
   from
   (select mbrs_source_system_key , effective_start_datetime, count(*) as dupe_count
   from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`
   group by 1, 2
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_comment'  ) );

 ---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a.mbrs_adw_key ) from
 (select mbrs_adw_key, mbrs_source_system_key , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`) a
 join
 (select mbrs_adw_key, mbrs_source_system_key, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`) b
 on a.mbrs_adw_key=b.mbrs_adw_key
    and a.mbrs_source_system_key  = b.mbrs_source_system_key
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.mbrs_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_membership' ));