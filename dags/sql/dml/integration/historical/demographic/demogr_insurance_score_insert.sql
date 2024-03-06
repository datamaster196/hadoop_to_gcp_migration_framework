INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_ins_score`
  (
demo_ins_score_adw_key,
demo_person_adw_key,
demo_activity_tile,
demo_channel_pref_i_dcil,
demo_channel_pref_d_dcil,
demo_channel_pref_e_dcil,
demo_channel_pref,
demo_group_cd,
demo_life_attrin_mdl_dcil,
demo_mkt_rsk_clsfir_auto_dcil,
demo_life_class_dcil ,
demo_premium_dcil ,
demo_prospect_survival_dcil ,
demo_pred_rnw_month_auto ,
demo_pred_rnw_month_home ,
actv_ind,
integrate_snapshot_dt,
integrate_insert_datetime,
integrate_insert_batch_number
  )
SELECT
 pk.demo_ins_score_adw_key,
 COALESCE(source.demo_person_adw_key,'-1'),
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
'Y',
source.integrate_snapshot_dt,
CURRENT_DATETIME() ,
{{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_transformed` source
  LEFT JOIN
     (
       SELECT
          GENERATE_UUID() AS demo_ins_score_adw_key,
          demo_lexid
       FROM
         `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_insurance_score_work_source`
       WHERE  sequence = 1
       GROUP BY
         demo_lexid) pk
   	ON
     	source.demo_lexid=pk.demo_lexid
;
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for demo_person_adw_key

SELECT
     count(target.demo_person_adw_key) AS demo_person_adw_key_count
 FROM
     (select distinct demo_person_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_demo_ins_score`)  target
       where not exists (select 1
                      from (select distinct demo_person_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_demo_person`) source_FK_1
                          where target.demo_person_adw_key = source_FK_1.demo_person_adw_key)
HAVING
 IF((demo_person_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_demo_ins_score. FK Column: demo_person_adw_key'));
 
--   -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    demo_person_adw_key ,
    integrate_snapshot_dt,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_demo_ins_score`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.dim_demo_ins_score' ));
