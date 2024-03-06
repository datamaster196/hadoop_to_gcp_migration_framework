CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.demogr_person_work_source` AS
 SELECT  source.LexID as demo_lexid,
         --source.LexHHID as demogr_lexhhid,
         --source.memb_id as demogr_member_identifier,
         --source.ass_id as demogr_associate_identifier,
         source.exact_age_p1 demo_exact_age,
 	source.estimated_age_p1 demo_estimated_age,
         source.est_hh_income_v5 demo_hsd_estimated_income,
         source.Combined_Dwelling_Type  demo_dwelling_typ,
 	source.Combined_Gender  demo_gender,
 	source.Combined_Marital_Status  demo_marital_status,
 	Combined_DOB as demo_birth_dt,
 	source.Combined_Ownership_Code  demo_home_ownership_cd,
 	--source.est_hh_income_v5   demo_income_range_cd, --income_code_drv
 	source.Combined_Length_of_Residence demo_length_of_residence,
 	source.Combined_Presence_of_Children  demo_presence_of_children,
 	source.Age_drv  demo_age_group_cd,
 	source.Business_Owner_P1  demo_biz_owner,
 	source.Education_P1  demo_education_level,
 	source.occupation_code_polk  demo_occptn_cd,
 	source.occupation_group_p1_v2  demo_occptn_group, --P1_Occupation_Group_V2
 	source.Ethnic_detail_code  demo_ethnic_detail_cd,
 	source.Ethnic_religion_code  demo_ethnic_religion_cd,
 	source.Ethnic_language_pref  demo_ethnic_language_pref,
 	source.ethnic_grouping_code  demo_ethnic_grouping_cd,
 	source.Ethnic_country_of_origin_code  demo_ethnic_cntry_of_origin_cd,
 	source.adw_lake_insert_datetime,
 	row_number() OVER (partition BY LexID ORDER BY LexID DESC) AS sequence
 from `{{ var.value.INGESTION_PROJECT }}.demographic.marketing_magnifier` AS source
where LexID <> '' and LexID IS NOT NULL
and DATE_SUB(DATE_TRUNC(cast( source.adw_lake_insert_datetime as date), MONTH), INTERVAL 1 DAY) NOT IN (SELECT DISTINCT integrate_snapshot_dt FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_demo_person` )
and source.adw_lake_insert_datetime > (select  cast(DATE_SUB(cast(max(adw_lake_insert_datetime) as date), INTERVAL 1 day) as datetime) from `{{ var.value.INGESTION_PROJECT }}.demographic.marketing_magnifier`)
