CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_transformed` AS
  SELECT
  demogr_person.demo_person_adw_key,
  source.demo_lexid,
  source.demogr_lexhhid,
  --source.demogr_member_identifier,
  --source.demogr_associate_identifier,
  source.demo_activity_tile,
  source.demo_channel_pref_i_dcil,
  source.demo_channel_pref_d_dcil,
  source.demo_channel_pref_e_dcil,
  source.demo_channel_pref,
  source.demo_group_cd,
  source.demo_life_attrin_mdl_dcil,
  source.demo_mkt_rsk_clsfir_auto_dcil ,
  source.demo_life_class_dcil,
  source.demo_premium_dcil,
  source.demo_prospect_survival_dcil,
  source.demo_pred_rnw_month_auto ,
  source.demo_pred_rnw_month_home ,
  DATE_SUB(DATE_TRUNC(cast( source.adw_lake_insert_datetime as date), MONTH), INTERVAL 1 DAY) as integrate_snapshot_dt
 FROM
     `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_source` AS source  LEFT JOIN
     `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person` as demogr_person
  	ON
    	demogr_person.demo_lexid = source.demo_lexid
    	AND demogr_person.actv_ind='Y'
 WHERE  sequence = 1
