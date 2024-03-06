CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.outbound_dim_demo_person_work_source`
as select demo_person_adw_key, contact_adw_key, demo_exact_age, demo_estimated_age,
          demo_hsd_estimated_income, demo_dwelling_typ, demo_gender, demo_marital_status,
          demo_birth_dt, demo_home_ownership_cd, demo_income_range_cd, demo_length_of_residence,
          demo_presence_of_children, demo_age_group_cd, demo_biz_owner, demo_education_level,
          demo_occptn_cd, demo_occptn_group, demo_ethnic_detail_cd, demo_ethnic_religion_cd,
          demo_ethnic_language_pref, demo_ethnic_grouping_cd, demo_ethnic_cntry_of_origin_cd,
          demo_lexid, actv_ind, integrate_snapshot_dt, integrate_insert_datetime, integrate_insert_batch_number
 from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person` where actv_ind = 'Y'  limit 10
