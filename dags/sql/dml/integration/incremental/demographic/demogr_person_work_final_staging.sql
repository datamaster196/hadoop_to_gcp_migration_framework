CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_final_stage` AS
SELECT
 COALESCE(target.demo_person_adw_key,GENERATE_UUID()) AS demo_person_adw_key,
 source.demo_lexid,
 --source.demogr_member_identifier,
 --source.demogr_associate_identifier,
 COALESCE(source.contact_adw_key,'-1') as contact_adw_key,
 source.demo_exact_age,
 source.demo_estimated_age,
 source.demo_hsd_estimated_income,
 source.demo_dwelling_typ,
 source.demo_gender,
 source.demo_marital_status,
 CAST(source.demo_birth_dt AS DATE) as demo_birth_dt,
 source.demo_home_ownership_cd,
 --source.est_hh_income_v5   demo_income_range_cd, --income_code_drv
 SAFE_CAST(source.demo_length_of_residence AS INT64) as demo_length_of_residence,
 source.demo_presence_of_children,
 source.demo_age_group_cd,
 source.demo_biz_owner,
 source.demo_education_level,
 source.demo_occptn_cd,
 source.demo_occptn_group, --P1_Occupation_Group_V2
 source.demo_ethnic_detail_cd,
 source.demo_ethnic_religion_cd,
 source.demo_ethnic_language_pref,
 source.demo_ethnic_grouping_cd,
 source.demo_ethnic_cntry_of_origin_cd,
    'Y' AS actv_ind,
    source.integrate_snapshot_dt,
    CURRENT_DATETIME() AS integrate_insert_datetime,
    {{ dag_run.id }} integrate_insert_batch_number
 FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_transformed` source LEFT JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person` target
  ON
    (source.demo_lexid = target.demo_lexid
     	--AND source.demogr_member_identifier = target.demogr_member_identifier
  	--AND source.demogr_associate_identifier = target.demogr_associate_identifier
      AND target.actv_ind='Y')
  --WHERE
--target.demo_lexid IS NULL
    --OR target.demogr_member_identifier IS NULL
    --OR target.demogr_associate_identifier
    --OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
 target.demo_person_adw_key,
 target.demo_lexid,
 --target.demogr_member_identifier,
 --target.demogr_associate_identifier,
 target.contact_adw_key,
 target.demo_exact_age,
 target.demo_estimated_age,
 target.demo_hsd_estimated_income,
 target.demo_dwelling_typ,
 target.demo_gender,
 target.demo_marital_status,
 target.demo_birth_dt,
 target.demo_home_ownership_cd,
 --target.est_hh_income_v5   demo_income_range_cd, --income_code_drv
 target.demo_length_of_residence,
 target.demo_presence_of_children,
 target.demo_age_group_cd,
 target.demo_biz_owner,
 target.demo_education_level,
 target.demo_occptn_cd,
 target.demo_occptn_group, --P1_Occupation_Group_V2
 target.demo_ethnic_detail_cd,
 target.demo_ethnic_religion_cd,
 target.demo_ethnic_language_pref,
 target.demo_ethnic_grouping_cd,
 target.demo_ethnic_cntry_of_origin_cd,
  'N' AS actv_ind,
  target.integrate_snapshot_dt,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number
FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_transformed` source INNER JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person` target
  ON
      (source.demo_lexid = target.demo_lexid
       	--AND source.demogr_member_identifier = target.demogr_member_identifier
    	--AND source.demogr_associate_identifier = target.demogr_associate_identifier
      AND target.actv_ind='Y')
 --where source.adw_row_hash <> target.adw_row_hash
