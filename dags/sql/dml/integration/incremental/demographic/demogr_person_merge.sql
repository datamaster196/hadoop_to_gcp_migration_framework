MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_final_stage` b
    ON a.demo_person_adw_key = b.demo_person_adw_key
    AND a.integrate_insert_datetime = b.integrate_insert_datetime
    WHEN NOT MATCHED THEN INSERT(
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
    ) VALUES
    (
b.demo_person_adw_key,
b.demo_lexid,
COALESCE(b.contact_adw_key,'-1'),
b.demo_exact_age,
b.demo_estimated_age,
b.demo_hsd_estimated_income,
b.demo_dwelling_typ,
b.demo_gender,
b.demo_marital_status,
b.demo_birth_dt,
b.demo_home_ownership_cd,
--b.demo_income_range_cd,
b.demo_length_of_residence,
b.demo_presence_of_children,
b.demo_age_group_cd,
b.demo_biz_owner,
b.demo_education_level,
b.demo_occptn_cd,
b.demo_occptn_group,
b.demo_ethnic_detail_cd,
b.demo_ethnic_religion_cd,
b.demo_ethnic_language_pref,
b.demo_ethnic_grouping_cd,
b.demo_ethnic_cntry_of_origin_cd,
b.actv_ind,
b.integrate_snapshot_dt,
b.integrate_insert_datetime,
b.integrate_insert_batch_number
    )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    --a.integrate_snapshot_dt = b.integrate_snapshot_dt,
    a.actv_ind = b.actv_ind
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
