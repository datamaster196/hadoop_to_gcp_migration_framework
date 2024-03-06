MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.office_work_stage` b
ON
  a.aca_office_adw_key=b.aca_office_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime
  WHEN NOT MATCHED THEN INSERT ( aca_office_adw_key, aca_office_address, aca_office_state_cd, aca_office_phone_nbr, aca_office_list_source_key, aca_office_nm, aca_office_status, aca_office_typ, mbrs_branch_cd, ins_department_cd, rs_asstc_communicator_center, car_care_loc_nbr, tvl_branch_cd, aca_office_division, retail_sales_office_cd, retail_manager, retail_mngr_email, retail_assistant_manager, retail_assistant_mngr_email, staff_assistant, staff_assistant_email, car_care_manager, car_care_mngr_email, car_care_assistant_manager, car_care_assistant_mngr_email, region_tvl_sales_mgr, region_tvl_sales_mgr_email, retail_sales_coach, retail_sales_coach_email, xactly_retail_store_nbr, xactly_car_care_store_nbr, region, car_care_general_manager, car_care_general_mngr_email, car_care_dirctr, car_care_dirctr_email, car_care_district_nbr, car_care_cost_center_nbr, tire_cost_center_nbr, admin_cost_center_nbr, occupancy_cost_center_nbr, retail_general_manager, retail_general_mngr_email, district_manager, district_mngr_email, retail_district_nbr, retail_cost_center_nbr, tvl_cost_center_nbr, mbrs_cost_center_nbr, facility_cost_center_nbr, national_ats_nbr, mbrs_division_ky,mbrs_division_nm, effective_start_datetime, effective_end_datetime, actv_ind, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number, adw_row_hash ) VALUES ( b.aca_office_adw_key, b.aca_office_address, b.aca_office_state_cd, b.aca_office_phone_nbr, b.aca_office_list_source_key, b.aca_office_nm, b.aca_office_status, b.aca_office_typ, b.mbrs_branch_cd, b.ins_department_cd, b.rs_asstc_communicator_center, b.car_care_loc_nbr, b.tvl_branch_cd, b.aca_store_division, b.retail_sales_office_cd, b.retail_manager, b.retail_mngr_email, b.retail_assistant_manager, b.retail_assistant_mngr_email, b.staff_assistant, b.staff_assistant_email, b.car_care_manager, b.car_care_mngr_email, b.car_care_assistant_manager, b.car_care_assistant_mngr_email, b.region_tvl_sales_mgr, b.region_tvl_sales_mgr_email, b.retail_sales_coach, b.retail_sales_coach_email, b.xactly_retail_store_nbr, b.xactly_car_care_store_nbr, b.region, b.car_care_general_manager, b.car_care_general_mngr_email, b.car_care_dirctr, b.car_care_dirctr_email, b.car_care_district_nbr, b.car_care_cost_center_nbr, b.tire_cost_center_nbr, b.admin_cost_center_nbr, b.occupancy_cost_center_nbr, b.retail_general_manager, b.retail_general_mngr_email, b.district_manager, b.district_mngr_email, b.retail_district_nbr, b.retail_cost_center_nbr, b.tvl_cost_center_nbr, b.mbrs_cost_center_nbr, b.facility_cost_center_nbr, b.national_ats_nbr, b.mbrs_division_ky, b.mbrs_division_nm, b.effective_start_datetime, b.effective_end_datetime, b.actv_ind, b.integrate_insert_datetime, b.integrate_insert_batch_number, b.integrate_update_datetime, b.integrate_update_batch_number, b.adw_row_hash )
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
-- No Orphaned foreign key check for dim_aca_office

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
    (select aca_office_list_source_key , effective_start_datetime, count(*) as dupe_count
    from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office`
    group by 1, 2
    having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_aca_office'  ) );

---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.aca_office_adw_key )
from
 (select aca_office_adw_key , aca_office_list_source_key , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office`) a
join
 (select aca_office_adw_key, aca_office_list_source_key, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_aca_office`) b
on a.aca_office_adw_key=b.aca_office_adw_key
    and a.aca_office_list_source_key  = b.aca_office_list_source_key
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.aca_office_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_aca_office' ));