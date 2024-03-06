#######################################################################################################################
##  DAG_NAME :  outbound_adw_cdl
##  PURPOSE :   Executes Outsbound DAG for CDL adw
##  PREREQUISITE DAGs :
#######################################################################################################################

from airflow.utils.dates import timezone
import pendulum
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable  # must be imported after ingestion_utilities
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

local_tz = pendulum.timezone("US/Eastern")


config_tables = {
   'dim_demo_ins_score':
    {
      'demo_ins_score_adw_key, demo_person_adw_key,demo_activity_tile,demo_channel_pref_i_dcil,demo_channel_pref_d_dcil,demo_channel_pref_e_dcil,demo_channel_pref,demo_group_cd,demo_life_attrin_mdl_dcil,demo_mkt_rsk_clsfir_auto_dcil,demo_life_class_dcil,demo_premium_dcil,demo_prospect_survival_dcil,demo_pred_rnw_month_auto,demo_pred_rnw_month_home,actv_ind,integrate_snapshot_dt,integrate_insert_datetime,integrate_insert_batch_number':"demo_ins_score_adw_key, demo_person_adw_key, demo_activity_tile, demo_channel_pref_i_dcil, demo_channel_pref_d_dcil, demo_channel_pref_e_dcil, demo_channel_pref, demo_group_cd, demo_life_attrin_mdl_dcil, demo_mkt_rsk_clsfir_auto_dcil, demo_life_class_dcil, demo_premium_dcil, demo_prospect_survival_dcil, demo_pred_rnw_month_auto, demo_pred_rnw_month_home, actv_ind, ifnull(safe_cast( IF( integrate_snapshot_dt < '1900-01-01', null, integrate_snapshot_dt) as string),'null') as integrate_snapshot_dt, integrate_insert_datetime, integrate_insert_batch_number"
    },
   'dim_demo_person':
   {
      'demo_person_adw_key, contact_adw_key, demo_exact_age, demo_estimated_age, demo_hsd_estimated_income, demo_dwelling_typ, demo_gender, demo_marital_status, demo_birth_dt, demo_home_ownership_cd, demo_income_range_cd, demo_length_of_residence, demo_presence_of_children, demo_age_group_cd, demo_biz_owner, demo_education_level, demo_occptn_cd, demo_occptn_group, demo_ethnic_detail_cd, demo_ethnic_religion_cd, demo_ethnic_language_pref, demo_ethnic_grouping_cd, demo_ethnic_cntry_of_origin_cd, demo_lexid, actv_ind, integrate_snapshot_dt, integrate_insert_datetime, integrate_insert_batch_number':"demo_person_adw_key, contact_adw_key, demo_exact_age, demo_estimated_age, demo_hsd_estimated_income, demo_dwelling_typ, demo_gender, demo_marital_status, ifnull(safe_cast( IF( demo_birth_dt < '1900-01-01', null, demo_birth_dt) as string),'null') as demo_birth_dt, demo_home_ownership_cd, demo_income_range_cd, demo_length_of_residence, demo_presence_of_children, demo_age_group_cd, demo_biz_owner, demo_education_level, demo_occptn_cd, demo_occptn_group, demo_ethnic_detail_cd, demo_ethnic_religion_cd, demo_ethnic_language_pref, demo_ethnic_grouping_cd, demo_ethnic_cntry_of_origin_cd, demo_lexid, actv_ind, ifnull(safe_cast( IF( integrate_snapshot_dt < '1900-01-01', null, integrate_snapshot_dt) as string),'null') as integrate_snapshot_dt, integrate_insert_datetime, integrate_insert_batch_number"
   },
   'dim_member':
   {
      'mbr_adw_key, mbrs_adw_key, contact_adw_key, mbrs_solicit_adw_key, emp_role_adw_key, mbr_source_system_key, mbrs_id, mbr_assoc_id, mbr_ck_dgt_nbr, mbr_status_cd, mbr_expiration_dt, mbr_card_expiration_dt, prim_mbr_cd, mbr_do_not_renew_ind, mbr_billing_cd, mbr_billing_category_cd, mbr_status_dtm, mbr_cancel_dt, mbr_join_aaa_dt, mbr_join_club_dt, mbr_reason_joined_cd, mbr_assoc_relation_cd, mbr_solicit_cd, mbr_source_of_sale_cd, mbr_comm_cd, mbr_future_cancel_ind, mbr_future_cancel_dt, mbr_renew_method_cd, free_mbr_ind, previous_club_mbrs_id, previous_club_cd, mbr_merged_previous_key, mbr_merged_previous_club_cd, mbr_mbrs_merged_previous_id, mbr_assoc_merged_previous_id, mbr_merged_previous_ck_dgt_nbr, mbr_merged_previous_mbr16_id, mbr_change_nm_dtm, mbr_email_changed_dtm, mbr_bad_email_ind, mbr_web_last_login_dtm, mbr_active_expiration_dt, mbr_actvtn_dt, mbr_duplicate_email_ind, mbr_dn_ask_for_email_ind, mbr_refused_give_email_ind, mbrs_purge_ind, payment_plan_future_cancel_dt, payment_plan_future_cancel_ind, mbr_card_typ_cd, mbr_card_typ_desc, entitlement_start_dt, entitlement_end_dt, mbr_previous_expiration_dt, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number':"mbr_adw_key, mbrs_adw_key, contact_adw_key, mbrs_solicit_adw_key, emp_role_adw_key, mbr_source_system_key, mbrs_id, mbr_assoc_id, mbr_ck_dgt_nbr, mbr_status_cd, ifnull(safe_cast( IF( mbr_expiration_dt < '1900-01-01', null, mbr_expiration_dt) as string),'null') as mbr_expiration_dt, ifnull(safe_cast( IF( mbr_card_expiration_dt < '1900-01-01', null, mbr_card_expiration_dt) as string),'null') as mbr_card_expiration_dt, prim_mbr_cd, mbr_do_not_renew_ind, mbr_billing_cd, mbr_billing_category_cd, ifnull(safe_cast( IF( mbr_status_dtm < '1900-01-01', null, mbr_status_dtm) as string),'null') as mbr_status_dtm, ifnull(safe_cast( IF( mbr_cancel_dt < '1900-01-01', null, mbr_cancel_dt) as string),'null') as mbr_cancel_dt, ifnull(safe_cast( IF( mbr_join_aaa_dt < '1900-01-01', null, mbr_join_aaa_dt) as string),'null') as mbr_join_aaa_dt, ifnull(safe_cast( IF( mbr_join_club_dt < '1900-01-01', null, mbr_join_club_dt) as string),'null') as mbr_join_club_dt, mbr_reason_joined_cd, mbr_assoc_relation_cd, mbr_solicit_cd, mbr_source_of_sale_cd, mbr_comm_cd, mbr_future_cancel_ind, ifnull(safe_cast( IF( mbr_future_cancel_dt < '1900-01-01', null, mbr_future_cancel_dt) as string),'null') as mbr_future_cancel_dt, mbr_renew_method_cd, free_mbr_ind, previous_club_mbrs_id, previous_club_cd, mbr_merged_previous_key, mbr_merged_previous_club_cd, mbr_mbrs_merged_previous_id, mbr_assoc_merged_previous_id, mbr_merged_previous_ck_dgt_nbr, mbr_merged_previous_mbr16_id, ifnull(safe_cast( IF( mbr_change_nm_dtm < '1900-01-01', null, mbr_change_nm_dtm) as string),'null') as mbr_change_nm_dtm, ifnull(safe_cast( IF( mbr_email_changed_dtm < '1900-01-01', null, mbr_email_changed_dtm) as string),'null') as mbr_email_changed_dtm, mbr_bad_email_ind, ifnull(safe_cast( IF( mbr_web_last_login_dtm < '1900-01-01', null, mbr_web_last_login_dtm) as string),'null') as mbr_web_last_login_dtm, ifnull(safe_cast( IF( mbr_active_expiration_dt < '1900-01-01', null, mbr_active_expiration_dt) as string),'null') as mbr_active_expiration_dt, ifnull(safe_cast( IF( mbr_actvtn_dt < '1900-01-01', null, mbr_actvtn_dt) as string),'null') as mbr_actvtn_dt, mbr_duplicate_email_ind, mbr_dn_ask_for_email_ind, mbr_refused_give_email_ind, mbrs_purge_ind, ifnull(safe_cast( IF( payment_plan_future_cancel_dt < '1900-01-01', null, payment_plan_future_cancel_dt) as string),'null') as payment_plan_future_cancel_dt, payment_plan_future_cancel_ind, mbr_card_typ_cd, mbr_card_typ_desc, ifnull(safe_cast( IF( entitlement_start_dt < '1900-01-01', null, entitlement_start_dt) as string),'null') as entitlement_start_dt, ifnull(safe_cast( IF( entitlement_end_dt < '1900-01-01', null, entitlement_end_dt) as string),'null') as entitlement_end_dt, ifnull(safe_cast( IF( mbr_previous_expiration_dt < '1900-01-01', null, mbr_previous_expiration_dt) as string),'null') as mbr_previous_expiration_dt, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number"
   },
   'dim_membership':
   {
      'mbrs_adw_key, address_adw_key, phone_adw_key, product_adw_key, aca_office_adw_key, mbrs_source_system_key, mbrs_id, mbrs_status_cd, mbrs_billing_cd, mbrs_status_dtm, mbrs_cancel_dt, mbrs_billing_category_cd, mbrs_dues_cost_amt, mbrs_dues_adj_amt, mbrs_future_cancel_ind, mbrs_future_cancel_dt, mbrs_out_of_territory_cd, mbrs_address_change_dtm, mbrs_cdx_export_update_dtm, mbrs_tier_key, mbrs_temporary_address_ind, previous_club_mbrs_id, previous_club_cd, mbrs_transfer_club_cd, mbrs_transfer_dtm, mbrs_merged_previous_club_cd, mbrs_mbrs_merged_previous_id, mbrs_mbrs_merged_previous_key, mbrs_dn_solicit_ind, mbrs_bad_address_ind, mbrs_dn_send_pubs_ind, mbrs_market_tracking_cd, mbrs_salvage_ind, mbrs_salvaged_ind, mbrs_dn_salvage_ind, mbrs_group_cd, mbrs_typ_cd, mbrs_ebill_ind, mbrs_marketing_segmntn_cd, mbrs_promo_cd, mbrs_phone_typ_cd, mbrs_ers_abuser_ind, mbrs_shadow_mcycle_ind, mbrs_student_ind, mbrs_address_ncoa_update_ind, mbrs_assigned_retention_ind, mbrs_mbrs_no_promotion_ind, mbrs_dn_offer_plus_ind, mbrs_aaawld_online_ed_ind, mbrs_shadowrv_coverage_ind, mbrs_heavy_ers_ind, mbrs_purge_ind, mbrs_unapplied_amt, mbrs_advance_payment_amt, mbrs_balance_amt, payment_plan_future_cancel_dt, payment_plan_future_cancel_ind, switch_prim_ind, supress_bill_ind, offer_end_dt, old_payment_plan_ind, carry_over_amt, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number':"mbrs_adw_key, address_adw_key, phone_adw_key, product_adw_key, aca_office_adw_key, mbrs_source_system_key, mbrs_id, mbrs_status_cd, mbrs_billing_cd, ifnull(safe_cast( IF( mbrs_status_dtm < '1900-01-01', null, mbrs_status_dtm) as string),'null') as mbrs_status_dtm, ifnull(safe_cast( IF( mbrs_cancel_dt < '1900-01-01', null, mbrs_cancel_dt) as string),'null') as mbrs_cancel_dt, mbrs_billing_category_cd, mbrs_dues_cost_amt, mbrs_dues_adj_amt, mbrs_future_cancel_ind, ifnull(safe_cast( IF( mbrs_future_cancel_dt < '1900-01-01', null, mbrs_future_cancel_dt) as string),'null') as mbrs_future_cancel_dt, mbrs_out_of_territory_cd, ifnull(safe_cast( IF( mbrs_address_change_dtm < '1900-01-01', null, mbrs_address_change_dtm) as string),'null') as mbrs_address_change_dtm, ifnull(safe_cast( IF( mbrs_cdx_export_update_dtm < '1900-01-01', null, mbrs_cdx_export_update_dtm) as string),'null') as mbrs_cdx_export_update_dtm, mbrs_tier_key, mbrs_temporary_address_ind, previous_club_mbrs_id, previous_club_cd, mbrs_transfer_club_cd, ifnull(safe_cast( IF( mbrs_transfer_dtm < '1900-01-01', null, mbrs_transfer_dtm) as string),'null') as mbrs_transfer_dtm, mbrs_merged_previous_club_cd, mbrs_mbrs_merged_previous_id, mbrs_mbrs_merged_previous_key, mbrs_dn_solicit_ind, mbrs_bad_address_ind, mbrs_dn_send_pubs_ind, mbrs_market_tracking_cd, mbrs_salvage_ind, mbrs_salvaged_ind, mbrs_dn_salvage_ind, mbrs_group_cd, mbrs_typ_cd, mbrs_ebill_ind, mbrs_marketing_segmntn_cd, mbrs_promo_cd, mbrs_phone_typ_cd, mbrs_ers_abuser_ind, mbrs_shadow_mcycle_ind, mbrs_student_ind, mbrs_address_ncoa_update_ind, mbrs_assigned_retention_ind, mbrs_mbrs_no_promotion_ind, mbrs_dn_offer_plus_ind, mbrs_aaawld_online_ed_ind, mbrs_shadowrv_coverage_ind, mbrs_heavy_ers_ind, mbrs_purge_ind, mbrs_unapplied_amt, mbrs_advance_payment_amt, mbrs_balance_amt, ifnull(safe_cast( IF( payment_plan_future_cancel_dt < '1900-01-01', null, payment_plan_future_cancel_dt) as string),'null') as payment_plan_future_cancel_dt, payment_plan_future_cancel_ind, switch_prim_ind, supress_bill_ind, ifnull(safe_cast( IF( offer_end_dt < '1900-01-01', null, offer_end_dt) as string),'null') as offer_end_dt, old_payment_plan_ind, carry_over_amt, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number"
   },
   'dim_member_rider':
   {
      'mbr_rider_adw_key, mbrs_adw_key, mbr_adw_key, mbr_ar_card_adw_key, emp_role_adw_key, product_adw_key, mbr_rider_source_key, mbr_rider_status_cd, mbr_rider_status_dtm, mbr_rider_cancel_dt, mbr_rider_cost_effective_dt, mbr_rider_solicit_cd, mbr_rider_do_not_renew_ind, mbr_rider_dues_cost_amt, mbr_rider_dues_adj_amt, mbr_rider_payment_amt, mbr_rider_paid_by_cd, mbr_rider_future_cancel_dt, mbr_rider_billing_category_cd, mbr_rider_cancel_reason_cd,mbr_rider_cancel_reason_desc, mbr_rider_reinstate_ind, mbr_rider_effective_dt, mbr_rider_original_cost_amt, mbr_rider_reinstate_reason_cd, mbr_rider_extend_exp_amt, mbr_rider_actual_cancel_dtm, mbr_rider_def_key, mbr_rider_actvtn_dt, effective_start_datetime, effective_end_datetime, actv_ind , adw_row_hash , integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number':"mbr_rider_adw_key, mbrs_adw_key, mbr_adw_key, mbr_ar_card_adw_key, emp_role_adw_key, product_adw_key, mbr_rider_source_key, mbr_rider_status_cd,  ifnull(safe_cast( IF( mbr_rider_status_dtm < '1900-01-01', null, mbr_rider_status_dtm) as string),'null') as mbr_rider_status_dtm,  ifnull(safe_cast( IF( mbr_rider_cancel_dt < '1900-01-01', null, mbr_rider_cancel_dt) as string),'null') as mbr_rider_cancel_dt,  ifnull(safe_cast( IF( mbr_rider_cost_effective_dt < '1900-01-01', null, mbr_rider_cost_effective_dt) as string),'null') as mbr_rider_cost_effective_dt, mbr_rider_solicit_cd, mbr_rider_do_not_renew_ind, mbr_rider_dues_cost_amt, mbr_rider_dues_adj_amt, mbr_rider_payment_amt, mbr_rider_paid_by_cd,  ifnull(safe_cast( IF( mbr_rider_future_cancel_dt < '1900-01-01', null, mbr_rider_future_cancel_dt) as string),'null') as mbr_rider_future_cancel_dt, mbr_rider_billing_category_cd, mbr_rider_cancel_reason_cd, mbr_rider_cancel_reason_desc, mbr_rider_reinstate_ind,  ifnull(safe_cast( IF( mbr_rider_effective_dt < '1900-01-01', null, mbr_rider_effective_dt) as string),'null') as mbr_rider_effective_dt, mbr_rider_original_cost_amt, mbr_rider_reinstate_reason_cd, mbr_rider_extend_exp_amt,  ifnull(safe_cast( IF( mbr_rider_actual_cancel_dtm < '1900-01-01', null, mbr_rider_actual_cancel_dtm) as string),'null') as mbr_rider_actual_cancel_dtm, mbr_rider_def_key,  ifnull(safe_cast( IF( mbr_rider_actvtn_dt < '1900-01-01', null, mbr_rider_actvtn_dt) as string),'null') as mbr_rider_actvtn_dt, effective_start_datetime, effective_end_datetime, actv_ind , adw_row_hash , integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number"
   },
   'dim_product':
   {
      'product_adw_key, product_category_adw_key, vendor_adw_key, product_sku, product_sku_key, product_sku_desc, product_sku_effective_dt, product_sku_expiration_dt, product_item_nbr_cd , product_item_desc, product_vendor_nm, product_service_category_cd, product_status_cd, product_dynamic_sku, product_dynamic_sku_desc, product_dynamic_sku_key, product_unit_cost, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number':"product_adw_key, product_category_adw_key, vendor_adw_key, replace(product_sku,'|','') as product_sku, trim(product_sku_key) as product_sku_key, trim(product_sku_desc) as product_sku_desc, ifnull(safe_cast( IF( product_sku_effective_dt < '1900-01-01', null, product_sku_effective_dt) as string),'null') as product_sku_effective_dt, ifnull(safe_cast( IF( product_sku_expiration_dt < '1900-01-01', null, product_sku_expiration_dt) as string),'null') as product_sku_expiration_dt, product_item_nbr_cd , product_item_desc, product_vendor_nm, product_service_category_cd, product_status_cd, product_dynamic_sku, product_dynamic_sku_desc, product_dynamic_sku_key, product_unit_cost, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number"
   },
   'sales_agent_activity':
   {
      'saa_adw_key, mbr_adw_key, mbrs_adw_key, mbrs_solicit_adw_key, aca_sales_office, aca_membership_office_adw_key, emp_role_adw_key,  product_adw_key,   mbrs_payment_apd_dtl_adw_key , saa_source_nm , saa_source_key ,   saa_tran_dt ,  mbr_comm_cd,   mbr_comm_desc, mbr_rider_cd,  mbr_rider_desc,mbr_expiration_dt, rider_solicit_cd,  rider_source_of_sale_cd,   sales_agent_tran_cd,   saa_dues_amt , saa_daily_billed_ind , rider_billing_category_cd, rider_billing_category_desc,   mbr_typ_cd, saa_commsable_activity_ind , saa_add_on_ind , saa_ar_ind , saa_fee_typ_cd ,   safety_fund_donation_ind,  saa_payment_plan_ind ,  zip_cd, saa_daily_cnt_ind , daily_cnt, effective_start_datetime,  effective_end_datetime,actv_ind,  adw_row_hash,  integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number':"saa_adw_key, mbr_adw_key, mbrs_adw_key, mbrs_solicit_adw_key, aca_sales_office, aca_membership_office_adw_key, emp_role_adw_key, product_adw_key, mbrs_payment_apd_dtl_adw_key, saa_source_nm, saa_source_key, ifnull(safe_cast( IF( saa_tran_dt < '1900-01-01', null, saa_tran_dt) as string),'null') as saa_tran_dt, mbr_comm_cd, mbr_comm_desc, mbr_rider_cd, mbr_rider_desc, ifnull(safe_cast( IF( mbr_expiration_dt < '1900-01-01', null, mbr_expiration_dt) as string),'null') as mbr_expiration_dt, rider_solicit_cd, rider_source_of_sale_cd, sales_agent_tran_cd, saa_dues_amt, saa_daily_billed_ind, rider_billing_category_cd, rider_billing_category_desc, mbr_typ_cd, saa_commsable_activity_ind, saa_add_on_ind, saa_ar_ind, saa_fee_typ_cd, safety_fund_donation_ind, saa_payment_plan_ind, zip_cd, saa_daily_cnt_ind, daily_cnt, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number"
   },
   'dim_contact_optout':
   {
      'contact_pref_adw_key, contact_adw_key, Topic_nm, channel, event_cd, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number':'contact_pref_adw_key, contact_adw_key, Topic_nm, channel, event_cd, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number'
   },
   'dim_aca_office':
    {
    'aca_office_adw_key,aca_office_list_source_key,aca_office_address,aca_office_state_cd,aca_office_phone_nbr,aca_office_nm,aca_office_status,aca_office_typ,mbrs_branch_cd,ins_department_cd,rs_asstc_communicator_center,car_care_loc_nbr,tvl_branch_cd,aca_office_division,retail_sales_office_cd,retail_manager,retail_mngr_email,retail_assistant_manager,retail_assistant_mngr_email,staff_assistant,staff_assistant_email,car_care_manager,car_care_mngr_email,car_care_assistant_manager,car_care_assistant_mngr_email,region_tvl_sales_mgr,region_tvl_sales_mgr_email,retail_sales_coach,retail_sales_coach_email,xactly_retail_store_nbr,xactly_car_care_store_nbr,region,car_care_general_manager,car_care_general_mngr_email,car_care_dirctr,car_care_dirctr_email,car_care_district_nbr,car_care_cost_center_nbr,tire_cost_center_nbr,admin_cost_center_nbr,occupancy_cost_center_nbr,retail_general_manager,retail_general_mngr_email,district_manager,district_mngr_email,retail_district_nbr,retail_cost_center_nbr,tvl_cost_center_nbr,mbrs_cost_center_nbr,facility_cost_center_nbr,national_ats_nbr,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number': "aca_office_adw_key,aca_office_list_source_key,aca_office_address,aca_office_state_cd,aca_office_phone_nbr,aca_office_nm,aca_office_status,aca_office_typ,mbrs_branch_cd,ins_department_cd,rs_asstc_communicator_center,car_care_loc_nbr,tvl_branch_cd,aca_office_division,retail_sales_office_cd,retail_manager,retail_mngr_email,retail_assistant_manager,retail_assistant_mngr_email,staff_assistant,staff_assistant_email,car_care_manager,car_care_mngr_email,car_care_assistant_manager,car_care_assistant_mngr_email,region_tvl_sales_mgr,region_tvl_sales_mgr_email,retail_sales_coach,retail_sales_coach_email,xactly_retail_store_nbr,xactly_car_care_store_nbr,region,car_care_general_manager,car_care_general_mngr_email,car_care_dirctr,car_care_dirctr_email,car_care_district_nbr,car_care_cost_center_nbr,tire_cost_center_nbr,admin_cost_center_nbr,occupancy_cost_center_nbr,retail_general_manager,retail_general_mngr_email,district_manager,district_mngr_email,retail_district_nbr,retail_cost_center_nbr,tvl_cost_center_nbr,mbrs_cost_center_nbr,facility_cost_center_nbr,national_ats_nbr,effective_start_datetime,effective_end_datetime,actv_ind,adw_row_hash,integrate_insert_datetime,integrate_insert_batch_number,integrate_update_datetime,integrate_update_batch_number"
    },
}

SOURCE_DB_NAME = 'adw'
DAG_TITLE = 'outbound_adw_cdl'
SCHEDULE = Variable.get("outbound_cdl_schedule", default_var='') or None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22, 0, tzinfo=local_tz),
    'email': [FAIL_EMAIL],
    'catchup': False,
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=360),
    'file_partition': iu.build_file_partition(),
    'params': {
        'dag_name': DAG_TITLE
    }
}

def parse_value(dic):
   for (k, v) in dic.items():
       column =  k
       select = v
   return column, select

def read_hwm_table(table):

    client = iu._get_bq_client()

    query = f"""SELECT dataset_name, table_name, safe_cast(ifnull(safe_cast(highwatermark as date), DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY)) as string) as highwatermark  FROM `{int_u.INGESTION_PROJECT}.admin.ingestion_highwatermarks` where dataset_name = 'outbound' and table_name = '{ table }' """
    query_job = client.query(query, location="US")

    hwm_value = '{{ execution_date.strftime("%Y-%m-%d") }}'
    a = {}
    for row in query_job:  # expects [dataset_name, table_name, highwatermark_value]
        ds, tablenm, hwm = row[:3]
        a[tablenm] = hwm
        hwm_value = a.get(table, '')
    return hwm_value

dag = DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=3, max_active_runs=1)

# grab table configuration data and highwatermark control table data
config_cluster = iu.read_webserver_cluster_config(SOURCE_DB_NAME)

create_cluster = iu.create_dataproc_cluster(config_cluster, dag)
delete_cluster = iu.delete_dataproc_cluster(config_cluster, dag)

for table, fields in config_tables.items():

    column, select = parse_value(fields)
    hwm_date = read_hwm_table (table)

    if table == 'sales_agent_activity':
        create_table_sql = f"""
                           CREATE or REPLACE table `{ int_u.INTEGRATION_PROJECT }.adw_work.outbound_{ table }_work_source`  as select { select } from `{ int_u.INTEGRATION_PROJECT }.adw.{ table }` where actv_ind = 'Y' and safe_cast(saa_tran_dt as date) <= CURRENT_DATE() and safe_cast(saa_tran_dt as date) > (select ifnull(safe_cast(highwatermark as date), DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY)) from `{ int_u.INGESTION_PROJECT }.admin.ingestion_highwatermarks` where dataset_name = 'outbound' and table_name = '{table}')
                           """
        setupsql = f"""
                    Delete from { table } where saa_tran_dt > convert(date,'{hwm_date}')
                    """
        hwm_update = f"""
           update `{int_u.INGESTION_PROJECT}.admin.ingestion_highwatermarks` set highwatermark = ( select safe_cast(ifnull(max(safe_cast(saa_tran_dt as date)),CURRENT_DATE()) as string) from  `{int_u.INTEGRATION_PROJECT}.adw_work.outbound_{table}_work_source` where  safe_cast(saa_tran_dt as date) <= CURRENT_DATE() ) 
           where dataset_name = 'outbound' and table_name = '{ table }'
           """
    else:
        create_table_sql = f"""
                           CREATE or REPLACE table `{ int_u.INTEGRATION_PROJECT }.adw_work.outbound_{ table }_work_source`  as select { select } from `{ int_u.INTEGRATION_PROJECT }.adw.{ table }` where actv_ind = 'Y' 
                           """
        setupsql = f"""
                    TRUNCATE TABLE {table}
                    """
        hwm_update = f"""
                 update `{int_u.INGESTION_PROJECT}.admin.ingestion_highwatermarks` set highwatermark = safe_cast(CURRENT_DATE() as string)
                 where dataset_name = 'outbound' and table_name = '{ table }'
                 """

    prepare_work_source = BigQueryOperator(task_id=f"prepare_{table}_work_source",
                                           #sql= "CREATE or REPLACE table `{{ params.integration_project }}.adw_work.outbound_{{ params.table }}_work_source`  as select {{ params.fields }} from `{{ params.integration_project }}.adw.{{ params.table }}` where actv_ind = 'Y' ",
                                           sql=create_table_sql,
                                           dag=dag,
                                           use_legacy_sql=False,
                                           retries=0,
                                           params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                                   'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                                   'table': table,
                                                   'fields': select})
    bq_2_gcs = BashOperator(task_id=f"bq_2_gcs_{table}",
                             bash_command='bq extract --project_id={{ params.integration_project }}  --field_delimiter "|" --print_header=false adw_work.outbound_{{ params.table }}_work_source gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}/{{ params.table }}_*.psv',
                             dag=dag,
                             params= {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                      'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                      'table': table,
                                      'fields': column})
    audit_start_date = BashOperator(task_id=f"audit_start_date_{table}",
                                    bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-bq"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "update dbo.cdl_load_status  set [load_start_dtm] = CURRENT_TIMESTAMP, [load_complete_dtm] = NULL where table_name = '"'"'{{ params.table }}'"'"' "    ',
                                    dag=dag,
                                    params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                            'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                            'environment': Variable.get("environment"),
                                            'Cdl_Server': Variable.get("Cdl_Server"),
                                            'Cdl_db': Variable.get("Cdl_db"),
                                            'table': table,
                                            'fields': column})

    setup_sql = BashOperator(task_id=f"setup_sql_{table}",
                             #bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-bq"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_ingest_utilities/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_ingest_utilities/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_ingest_utilities/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName=adw_cdl;" --username="SVCGCPUSER"  --password-file="gs://adw_ingest_utilities/source_db_account_credentials/cdl/dev/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "TRUNCATE TABLE {{ params.table }}"    ',
                             bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-bq"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "{{ params.sql }}"    ',
                             dag=dag,
                             params = {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                       'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                       'environment': Variable.get("environment"),
                                       'Cdl_Server':Variable.get("Cdl_Server"),
                                       'Cdl_db': Variable.get("Cdl_db"),
                                       'table': table,
                                       'fields': column,
                                       'sql': setupsql})
    gcs_2_sql = BashOperator(task_id=f"gcs_2_sql_{table}",
                              bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}  --cluster="sqoop-gcp-outbound-bq"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- export -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --table "{{ params.table }}" --export-dir "gs://{{ params.integration_project }}-outgoing/{{ params.table }}/{{ execution_date.strftime("%Y%m%d") }}" -m 9 -input-fields-terminated-by "|" --input-null-string "null" --input-null-non-string "-1" --columns  "{{ params.fields }}" ',
                              dag=dag,
                              params= {'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                       'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                       'environment': Variable.get("environment"),
                                       'Cdl_Server':Variable.get("Cdl_Server"),
                                       'Cdl_db': Variable.get("Cdl_db"),
                                       'table': table,
                                       'fields': column})
    audit_complete_date = BashOperator(task_id=f"audit_complete_date_{table}",
                                       bash_command='gcloud dataproc jobs submit hadoop  --project={{ params.ingestion_project }}   --cluster="sqoop-gcp-outbound-bq"  --region="us-east1"  --class=org.apache.sqoop.Sqoop   --jars="gs://adw_admin_db_connections/ingestion_jars/sqoop-1.4.7-hadoop260.jar,gs://adw_admin_db_connections/ingestion_jars/avro-tools-1.8.2.jar,gs://adw_admin_db_connections/ingestion_jars/jtds-1.3.1.jar"  -- eval -Dmapreduce.job.user.classpath.first=true  --connect="jdbc:jtds:sqlserver://{{ params.Cdl_Server }}.aaacorp.com:1433;DOMAIN=aaacorp;useNTLMv2=true;databaseName={{ params.Cdl_db }};" --username="SVCGCPUSER"  --password-file="gs://adw_admin_db_connections/source_db_account_credentials/cdl/{{ params.environment }}/cdl_ro_user_pw.txt" --connection-manager="org.apache.sqoop.manager.SQLServerManager" --driver="net.sourceforge.jtds.jdbc.Driver"   --query "update dbo.cdl_load_status  set [load_complete_dtm] = CURRENT_TIMESTAMP where table_name = '"'"'{{ params.table }}'"'"' "    ',
                                       dag=dag,
                                       params={'ingestion_project': Variable.get("INGESTION_PROJECT"),
                                               'integration_project': Variable.get("INTEGRATION_PROJECT"),
                                               'environment': Variable.get("environment"),
                                               'Cdl_Server': Variable.get("Cdl_Server"),
                                               'Cdl_db': Variable.get("Cdl_db"),
                                               'table': table,
                                               'fields': column})


    HWMUpdate = BigQueryOperator(task_id=f"HWMUpdate_{table}",
                                sql=hwm_update,
                                use_legacy_sql=False,
                                start_date=datetime(2019, 8, 22),
                                retries=0)

    create_cluster >> prepare_work_source >> bq_2_gcs >> audit_start_date >> setup_sql >> gcs_2_sql >> audit_complete_date >> HWMUpdate >> delete_cluster

AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([delete_cluster])