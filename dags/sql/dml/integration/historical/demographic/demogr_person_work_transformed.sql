CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_transformed` AS
 SELECT
 source.demo_lexid,
 adw_contact.contact_adw_key,
 source.demo_exact_age,
 source.demo_estimated_age,
 source.demo_hsd_estimated_income,
 source.demo_dwelling_typ, 
 source.demo_gender,
 source.demo_marital_status,
 case when CHARACTER_LENGTH(source.demo_birth_dt) = 8
           then concat(SUBSTR(source.demo_birth_dt,0,4),"-",SUBSTR (source.demo_birth_dt,5,2), "-",SUBSTR (source.demo_birth_dt,7,2))
      when CHARACTER_LENGTH(source.demo_birth_dt)=6
           then concat(SUBSTR(source.demo_birth_dt,0,4),"-", SUBSTR (source.demo_birth_dt,5,2),"-01")
      when CHARACTER_LENGTH(source.demo_birth_dt)=4
           then concat(SUBSTR(source.demo_birth_dt,0,4),"-01-01")
      else null 
 end as demo_birth_dt,
 source.demo_home_ownership_cd,
 source.demo_length_of_residence, 
 source.demo_presence_of_children, 
 source.demo_age_group_cd, 
 source.demo_biz_owner, 
 source.demo_education_level, 
 source.demo_occptn_cd, 
 source.demo_occptn_group, 
 source.demo_ethnic_detail_cd,
 source.demo_ethnic_religion_cd, 
 source.demo_ethnic_language_pref, 
 source.demo_ethnic_grouping_cd, 
 source.demo_ethnic_cntry_of_origin_cd,
 DATE_SUB(DATE_TRUNC(cast( source.adw_lake_insert_datetime as date), MONTH), INTERVAL 1 DAY) as integrate_snapshot_dt
FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_source` AS source  LEFT JOIN
    (select source_1_key, 
     contact_adw_key,
     row_number() over(partition by source_1_key order by effective_end_datetime desc, effective_start_datetime desc, contact_adw_key) as rn
     from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`
     where key_typ_nm = 'lexid'
 	   AND contact_source_system_nm = 'market_magnifier'
 	   AND actv_ind='Y'
     AND source_1_key IS NOT NULL
     ) adw_contact
  	ON
 	adw_contact.source_1_key = demo_lexid and
     adw_contact.rn=1
 WHERE  sequence = 1