MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_customer` target
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_fact_customer_work_transformed` source
  ON
    (source.integrate_snapshot_dt = target.integrate_snapshot_dt and source.contact_adw_key = target.contact_adw_key)
  -- Update info for any record in current snapshot which continue to remain active in current snapshot
  WHEN MATCHED and source.adw_row_hash <> target.adw_row_hash THEN UPDATE
	   SET
         contact_adw_key                 =   source.contact_adw_key
         ,integrate_snapshot_dt       =  source.integrate_snapshot_dt
         ,active_client_ind       =  source.active_client_ind
         ,mbrs_active_donor       =  source.mbrs_active_donor
         ,mbrs_territory_ind       =  source.mbrs_territory_ind
         ,mbrs_actv_ind       =  source.membership_actv_ind
         ,mbrs_first_dt       =  source.mbrs_first_dt
         ,mbrs_last_dt       =  source.mbrs_last_dt
         ,mbrs_coverage_level       =  source.mbrs_coverage_level
         ,mbrs_mbr_cnt       =  source.mbrs_mbr_cnt
         ,ins_actv_ind       =  source.insurance_actv_ind
         ,ins_lifetime_policy_cnt       =  source.ins_lifetime_policy_cnt
         ,ins_active_policy_cnt       =  source.ins_active_policy_cnt
         ,ins_first_policy_dt       =  source.ins_first_policy_dt
         ,ins_latest_policy_dt       =  source.ins_latest_policy_dt
         ,cor_actv_ind       =  source.cor_actv_ind
         ,cor_service_cnt       =  source.cor_service_cnt
         ,cor_first_service_dt       =  source.cor_first_service_dt
         ,cor_latest_service_dt       =  source.cor_latest_service_dt
         ,retail_actv_ind       =  source.retail_actv_ind
         ,retail_client_service_cnt       =  source.retail_client_service_cnt
         ,retail_first_purchase_dt       =  source.retail_first_purchase_dt
         ,retail_latest_purchase_dt       =  source.retail_latest_purchase_dt
         ,tvl_actv_ind       =  source.travel_actv_ind
         ,tvl_booking_cnt       =  source.tvl_booking_cnt
         ,tvl_first_booking_dt       =  source.tvl_first_booking_dt
         ,tvl_latest_booking_dt       =  source.tvl_latest_booking_dt
         ,financial_service_actv_ind       =  source.financial_service_actv_ind
         ,financial_service_client_cnt       =  source.financial_service_client_cnt
         ,financial_service_first_dt       =  source.financial_service_first_dt
         ,financial_service_latest_dt       =  source.financial_service_latest_dt
         ,actv_ind       =  source.actv_ind
         ,adw_row_hash       =   source.adw_row_hash
         ,integrate_update_datetime       =   CURRENT_DATETIME()
         ,integrate_update_batch_number       =   {{ dag_run.id }}
   -- Deactivate any contact_adw_key in current snapshot which no longer show up in latest load (e.g. membership is no longer active/pending)
   WHEN NOT MATCHED BY SOURCE AND target.integrate_snapshot_dt  = DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) THEN      UPDATE
      SET
	    active_client_ind = 'N',
        mbrs_actv_ind = 'N',
        actv_ind = 'N',
        integrate_update_datetime = CURRENT_DATETIME(),
        integrate_update_batch_number = {{ dag_run.id }},
        adw_row_hash = TO_BASE64(MD5(CONCAT(ifnull(target.active_client_ind,
              ''),'|',ifnull(target.mbrs_active_donor,
              ''),'|',ifnull('N',
              ''),'|',ifnull('N',
              ''),'|',ifnull(safe_cast(target.mbrs_first_dt as string),
              ''),'|',ifnull(safe_cast(target.mbrs_last_dt as string),
              ''),'|',ifnull(target.mbrs_coverage_level,
              ''),'|',ifnull(safe_cast(target.mbrs_mbr_cnt as string),
              ''),'|',ifnull(target.ins_actv_ind,
              ''),'|',ifnull(safe_cast(target.ins_lifetime_policy_cnt as string),
              ''),'|',ifnull(safe_cast(target.ins_active_policy_cnt as string),
              ''),'|',ifnull(safe_cast(target.ins_first_policy_dt as string),
              ''),'|',ifnull(safe_cast(target.ins_latest_policy_dt as string),
              ''),'|',ifnull(target.cor_actv_ind,
              ''),'|',ifnull(safe_cast(target.cor_service_cnt as string),
              ''),'|',ifnull(safe_cast(target.cor_first_service_dt as string),
              ''),'|',ifnull(safe_cast(target.cor_latest_service_dt as string),
              ''),'|',ifnull(target.retail_actv_ind,
              ''),'|',ifnull(safe_cast(target.retail_client_service_cnt as string),
              ''),'|',ifnull(safe_cast(target.retail_first_purchase_dt as string),
              ''),'|',ifnull(safe_cast(target.retail_latest_purchase_dt as string),
              ''),'|',ifnull(target.tvl_actv_ind,
              ''),'|',ifnull(safe_cast(target.tvl_booking_cnt as string),
              ''),'|',ifnull(safe_cast(target.tvl_first_booking_dt as string),
              ''),'|',ifnull(safe_cast(target.tvl_latest_booking_dt as string),
              ''),'|',ifnull(target.financial_service_actv_ind,
              ''),'|',ifnull(safe_cast(target.financial_service_client_cnt as string),
              ''),'|',ifnull(safe_cast(target.financial_service_first_dt as string),
              ''),'|',ifnull(safe_cast(target.financial_service_latest_dt as string),
              ''),'|',ifnull('N',
              '') )))

   -- Deactivate any older active snapshots
   WHEN NOT MATCHED BY SOURCE AND target.integrate_snapshot_dt <> DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) and target.actv_ind = 'Y' THEN UPDATE
      SET
        actv_ind = 'N',
        integrate_update_datetime = CURRENT_DATETIME(),
        integrate_update_batch_number = {{ dag_run.id }}
   -- Insert any new incoming records for current snapshot
   WHEN NOT MATCHED BY TARGET THEN INSERT
        (
         contact_adw_key
         ,integrate_snapshot_dt
         ,active_client_ind
         ,mbrs_active_donor
         ,mbrs_territory_ind
         ,mbrs_actv_ind
         ,mbrs_first_dt
         ,mbrs_last_dt
         ,mbrs_coverage_level
         ,mbrs_mbr_cnt
         ,ins_actv_ind
         ,ins_lifetime_policy_cnt
         ,ins_active_policy_cnt
         ,ins_first_policy_dt
         ,ins_latest_policy_dt
         ,cor_actv_ind
         ,cor_service_cnt
         ,cor_first_service_dt
         ,cor_latest_service_dt
         ,retail_actv_ind
         ,retail_client_service_cnt
         ,retail_first_purchase_dt
         ,retail_latest_purchase_dt
         ,tvl_actv_ind
         ,tvl_booking_cnt
         ,tvl_first_booking_dt
         ,tvl_latest_booking_dt
         ,financial_service_actv_ind
         ,financial_service_client_cnt
         ,financial_service_first_dt
         ,financial_service_latest_dt
         ,actv_ind
         ,adw_row_hash
         ,integrate_insert_datetime
         ,integrate_insert_batch_number
         ,integrate_update_datetime
         ,integrate_update_batch_number
         )
      VALUES
         (
          source.contact_adw_key
         ,source.integrate_snapshot_dt
         ,source.active_client_ind
         ,source.mbrs_active_donor
         ,source.mbrs_territory_ind
         ,source.membership_actv_ind
         ,source.mbrs_first_dt
         ,source.mbrs_last_dt
         ,source.mbrs_coverage_level
         ,source.mbrs_mbr_cnt
         ,source.insurance_actv_ind
         ,source.ins_lifetime_policy_cnt
         ,source.ins_active_policy_cnt
         ,source.ins_first_policy_dt
         ,source.ins_latest_policy_dt
         ,source.cor_actv_ind
         ,source.cor_service_cnt
         ,source.cor_first_service_dt
         ,source.cor_latest_service_dt
         ,source.retail_actv_ind
         ,source.retail_client_service_cnt
         ,source.retail_first_purchase_dt
         ,source.retail_latest_purchase_dt
         ,source.travel_actv_ind
         ,source.tvl_booking_cnt
         ,source.tvl_first_booking_dt
         ,source.tvl_latest_booking_dt
         ,source.financial_service_actv_ind
         ,source.financial_service_client_cnt
         ,source.financial_service_first_dt
         ,source.financial_service_latest_dt
         ,source.actv_ind
         ,source.adw_row_hash
         ,CURRENT_DATETIME()
         ,{{ dag_run.id }}
         ,CURRENT_DATETIME()
         ,{{ dag_run.id }}
       )
;

----------------------------------------------------------------------------------------------
-- Validations
-- adw.fact_customer: Post Load validation checks
-- Orphaned foreign key check

SELECT
     count(a.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
	    from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_customer`)  a
       where not exists (select 1
	                       from (select distinct contact_adw_key
						           from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`) b
                          where a.contact_adw_key = b.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0 ), true, ERROR('Error: FK check failed for adw.fact_customer. FK Column: contact_adw_key'));

----------------------------------------------------------------------------------------------
-- Duplicate Checks

 select count(1)
   from
   (select contact_adw_key , integrate_snapshot_dt, count(*) as dupe_count
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_customer`
   group by 1, 2
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.fact_customer'  ) );

 ---------------------------------------------------------------------------------------------
-- Active Indicator Check

select sum(x.contact_adw_key_count) as summed_contact_adw_key_count
from
(
select count(a.contact_adw_key) as contact_adw_key_count from
 (select contact_adw_key, integrate_snapshot_dt, actv_ind from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_customer`) a
 join
 (select contact_adw_key, max(integrate_snapshot_dt)  as integrate_snapshot_dt from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_customer` group by contact_adw_key) b
 on a.contact_adw_key=b.contact_adw_key
    and a.integrate_snapshot_dt  < b.integrate_snapshot_dt
    and coalesce(a.actv_ind, 'Y') = 'Y'
UNION all
select case when count(a.contact_adw_key) > 0 then 0 else 1 end as contact_adw_key_count from
 (select contact_adw_key, integrate_snapshot_dt, actv_ind from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_customer`) a
where exists
  (select 1 from
    (select contact_adw_key, max(integrate_snapshot_dt)  as integrate_snapshot_dt from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_customer`  group by contact_adw_key) b
       where a.contact_adw_key=b.contact_adw_key
         and a.integrate_snapshot_dt  = b.integrate_snapshot_dt
         and a.actv_ind = 'Y')
) x
HAVING IF ((sum(x.contact_adw_key_count) = 0), true, ERROR( 'Error: Active Indicator - "Y" check failed for adw.fact_customer' ));