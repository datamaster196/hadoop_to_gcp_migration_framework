INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person`
  (
demo_person_adw_key,
demo_lexid,
contact_adw_key,
demo_exact_age,
demo_estimated_age,
demo_hsd_estimated_income,
demo_dwelling_typ,
demo_gender,
demo_marital_status,
demo_birth_dt,
demo_home_ownership_cd,
--demo_income_range_cd,
demo_length_of_residence,
demo_presence_of_children,
demo_age_group_cd,
demo_biz_owner,
demo_education_level,
demo_occptn_cd,
demo_occptn_group,
demo_ethnic_detail_cd,
demo_ethnic_religion_cd,
demo_ethnic_language_pref,
demo_ethnic_grouping_cd,
demo_ethnic_cntry_of_origin_cd,
actv_ind,
integrate_snapshot_dt,
integrate_insert_datetime,
integrate_insert_batch_number
  )
SELECT
 pk.demo_person_adw_key,
 source.demo_lexid,
 COALESCE(source.contact_adw_key,'-1'),
 source.demo_exact_age,
 source.demo_estimated_age,
 source.demo_hsd_estimated_income,
 source.demo_dwelling_typ,
 source.demo_gender,
 source.demo_marital_status,
 CAST(source.demo_birth_dt AS DATE),
 source.demo_home_ownership_cd,
 SAFE_CAST(source.demo_length_of_residence AS INT64),
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
'Y',
source.integrate_snapshot_dt,
CURRENT_DATETIME() ,
{{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_transformed` source
  LEFT JOIN
     (
       SELECT
          GENERATE_UUID() AS demo_person_adw_key,
          demo_lexid
       FROM
         `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_source`
       WHERE  sequence = 1
       GROUP BY
         demo_lexid
         ) pk
   	ON
     	source.demo_lexid=pk.demo_lexid
;
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for mbrs_adw_key

SELECT
     count(target.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_demo_person`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`) source_FK_1
                          where target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_demo_person. FK Column: contact_adw_key'));
 
--   -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    demo_lexid ,
    integrate_snapshot_dt,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_demo_person`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.dim_demo_person' ));
