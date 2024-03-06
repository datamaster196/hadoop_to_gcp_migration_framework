CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_ins_score`(
    demo_ins_score_adw_key           String   NOT NULL,
    demo_person_adw_key              String      NOT NULL,
    demo_activity_tile               String       ,
    demo_channel_pref_i_dcil       String       ,
    demo_channel_pref_d_dcil       String       ,
    demo_channel_pref_e_dcil       String       ,
    demo_channel_pref                String       ,
    demo_group_cd                    String       ,
    demo_life_attrin_mdl_dcil      String       ,
    demo_mkt_rsk_clsfir_auto_dcil    String       ,
    demo_life_class_dcil           String       ,
    demo_premium_dcil              String       ,
    demo_prospect_survival_dcil    String       ,
    demo_pred_rnw_month_auto         String       ,
    demo_pred_rnw_month_home         String       ,
    actv_ind                         String       NOT NULL,
    integrate_snapshot_dt            date           NOT NULL,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            NOT NULL
)
PARTITION BY (integrate_snapshot_dt)
CLUSTER BY actv_ind
;