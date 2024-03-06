CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_final_stage` AS
SELECT
 COALESCE(target.demo_ins_score_adw_key,GENERATE_UUID()) AS demo_ins_score_adw_key,
 --source.demogr_member_identifier,
 --source.demogr_associate_identifier,
 COALESCE(source.demo_person_adw_key,'-1') as demo_person_adw_key,
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
    'Y' AS actv_ind,
        source.integrate_snapshot_dt,
        CURRENT_DATETIME() AS integrate_insert_datetime,
    {{ dag_run.id }} integrate_insert_batch_number
 FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_transformed` source LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_ins_score` target
  ON
    (source.demo_person_adw_key = target.demo_person_adw_key
      AND target.actv_ind='Y')
UNION ALL
SELECT
 target.demo_ins_score_adw_key,
 --target.demogr_member_identifier,
 --target.demogr_associate_identifier,
 target.demo_person_adw_key,
 target.demo_activity_tile,
 target.demo_channel_pref_i_dcil,
 target.demo_channel_pref_d_dcil,
 target.demo_channel_pref_e_dcil,
 target.demo_channel_pref,
 target.demo_group_cd,
 target.demo_life_attrin_mdl_dcil,
 target.demo_mkt_rsk_clsfir_auto_dcil ,
 target.demo_life_class_dcil,
 target.demo_premium_dcil,
 target.demo_prospect_survival_dcil,
 target.demo_pred_rnw_month_auto ,
 target.demo_pred_rnw_month_home ,
    'N' AS actv_ind,
      target.integrate_snapshot_dt,
      target.integrate_insert_datetime,
  target.integrate_insert_batch_number
FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_transformed` source INNER JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_ins_score` target
  ON
      (source.demo_person_adw_key = target.demo_person_adw_key
      AND target.actv_ind='Y')
