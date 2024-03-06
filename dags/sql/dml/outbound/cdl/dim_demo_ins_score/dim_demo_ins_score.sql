CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.outbound_dim_demo_ins_score_work_source`
as select demo_ins_score_adw_key, demo_person_adw_key,demo_activity_tile,demo_channel_pref_i_dcil,demo_channel_pref_d_dcil,
          demo_channel_pref_e_dcil,demo_channel_pref,demo_group_cd,demo_life_attrin_mdl_dcil,demo_mkt_rsk_clsfir_auto_dcil,demo_life_class_dcil,
          demo_premium_dcil,demo_prospect_survival_dcil,demo_pred_rnw_month_auto,demo_pred_rnw_month_home,actv_ind,integrate_snapshot_dt,
          integrate_insert_datetime,integrate_insert_batch_number
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_ins_score` where actv_ind = 'Y'  limit 10

