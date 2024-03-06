CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_source` AS
 SELECT  source.LexID as demo_lexid,
        source.LexHHID as demogr_lexhhid,
        --source.memb_id as demogr_member_identifier,
        --source.ass_id as demogr_associate_identifier,
        source.Activity_tile as demo_activity_tile,
	source.Channel_Pref_I_Decile as demo_channel_pref_i_dcil,
	source.Channel_Pref_D_Decile as demo_channel_pref_d_dcil,
	source.Channel_Pref_E_Decile as demo_channel_pref_e_dcil,
	source.Channel_Preference as demo_channel_pref,
	source.Group_Code as demo_group_cd,
	source.Life_Attrition_Model_Decile as demo_life_attrin_mdl_dcil,
	source.marketing_attract_index as demo_mkt_rsk_clsfir_auto_dcil ,
	source.Life_Classification_Decile as demo_life_class_dcil,
	source.premium_decile as demo_premium_dcil,
	source.ProspectSurvival_Decile as demo_prospect_survival_dcil,
	source.prd_auto_month as demo_pred_rnw_month_auto ,
	source.prd_prop_month as demo_pred_rnw_month_home ,
	source.adw_lake_insert_datetime,
	row_number() OVER (partition BY source.LexID ORDER BY source.LexID DESC) AS sequence
from `{{ var.value.INGESTION_PROJECT }}.demographic.marketing_magnifier` AS source
where LexID <> '' and LexID IS NOT NULL
and DATE_SUB(DATE_TRUNC(cast( source.adw_lake_insert_datetime as date), MONTH), INTERVAL 1 DAY) NOT IN (SELECT DISTINCT integrate_snapshot_dt FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_ins_score` )
and source.adw_lake_insert_datetime > (select  cast(DATE_SUB(cast(max(adw_lake_insert_datetime) as date), INTERVAL 1 day) as datetime) from `{{ var.value.INGESTION_PROJECT }}.demographic.marketing_magnifier`)
