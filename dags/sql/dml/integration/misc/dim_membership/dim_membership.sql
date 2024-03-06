INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`
  ( mbrs_adw_key,
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
  --  mbrs_coverage_level_code,
  --  mbrs_coverage_level_key,
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
    integrate_update_batch_number )
SELECT
  mbrs_adw_key,
  source.address_adw_key,
  CAST(source.phone_adw_key AS string),
  CAST (source.product_adw_key AS string),
  CAST(source.aca_office_adw_key AS string),
  CAST(source.mbrs_source_system_key AS int64),
  source.mbrs_id ,
  source.mbrs_status_cd,
  source.mbrs_billing_cd,
  CAST(source.mbrs_status_dtm AS datetime),
  CAST(substr(source.mbrs_cancel_dt, 0, 10) AS date),
  source.mbrs_billing_category_cd,
  CAST(source.mbrs_dues_cost_amt AS numeric),
  CAST(source.mbrs_dues_adj_amt AS numeric),
  source.mbrs_future_cancel_ind,
  CAST(substr(source.mbrs_future_cancel_dt, 0, 10) AS date),
  source.mbrs_out_of_territory_cd,
  CAST(source.mbrs_address_change_dtm AS datetime),
  CAST(source.mbrs_cdx_export_update_dtm AS datetime),
  CAST(source.mbrs_tier_key AS float64),
  source.mbrs_temporary_address_ind,
  source.previous_club_mbrs_id ,
  source.previous_club_cd,
  source.mbrs_transfer_club_cd,
  CAST(source.mbrs_transfer_dtm AS datetime),
  CAST(source.mbrs_merged_previous_club_cd AS string),
  CAST(source.mbrs_mbrs_merged_previous_id  AS string),
  safe_cast(source.mbrs_mbrs_merged_previous_key as int64) as mbrs_mbrs_merged_previous_key,
  source.mbrs_dn_solicit_ind,
  source.mbrs_bad_address_ind,
  source.mbrs_dn_send_pubs_ind,
  source.mbrs_market_tracking_cd,
  source.mbrs_salvage_ind,
  source.mbrs_salvaged_ind,
  source.mbrs_dn_salvage_ind,
  --source.mbrs_coverage_level_code,
  --CAST(source.mbrs_coverage_level_key AS int64),
  source.mbrs_group_cd,
  source.mbrs_typ_cd,
  source.mbrs_ebill_ind,
  source.mbrs_marketing_segmntn_cd,
  source.mbrs_promo_cd,
  source.mbrs_phone_typ_cd,
  source.mbrs_ers_abuser_ind,
  source.mbrs_shadow_mcycle_ind,
  source.mbrs_student_ind,
  source.mbrs_address_ncoa_update_ind,
  source.mbrs_assigned_retention_ind,
  source.mbrs_mbrs_no_promotion_ind,
  source.mbrs_dn_offer_plus_ind,
  source.mbrs_aaawld_online_ed_ind,
  source.mbrs_shadowrv_coverage_ind,
  source.mbrs_heavy_ers_ind,
  CAST(source.mbrs_purge_ind AS string),
  CAST(source.mbrs_unapplied_amt AS numeric),
  CAST(source.mbrs_advance_payment_amt AS numeric),
  0 as mbrs_balance_amt,
  CAST(substr(source.payment_plan_future_cancel_dt, 0, 10) AS date),
  source.payment_plan_future_cancel_ind,
  source.switch_prim_ind,
  source.supress_bill_ind,
  CAST(substr(source.offer_end_dt, 0, 10) AS date),
  source.old_payment_plan_ind,
  CAST(source.carry_over_amt AS numeric),
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_work_transformed` source ON (
  source.mbrs_source_system_key=hist_type_2.mbrs_source_system_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    mbrs_adw_key,
    mbrs_source_system_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_membership_backup`
  GROUP BY
    mbrs_adw_key, mbrs_source_system_key) pk
ON (safe_cast(source.mbrs_source_system_key as int64)=pk.mbrs_source_system_key);

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
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_membership'  ) );

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