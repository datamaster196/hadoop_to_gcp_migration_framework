CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_segment_source`  as
SELECT
 membership_ky
,historysource.segmentation_setup_ky
,membership_exp_dt
,historysource.segmentation_test_end_dt
,historysource.segmentation_schedule_dt
,panel_cd
,segmentation_panel_cd
,control_panel_fl
,commission_cd
,setupsource.name as name
,ROW_NUMBER() OVER(PARTITION BY historysource.membership_ky
	                            ,historysource.membership_exp_dt
		              	        ,historysource.segmentation_panel_cd
                                ,setupsource.name
                    ORDER BY segmentation_history_ky DESC) AS dupe_check
FROM `{{ var.value.INGESTION_PROJECT }}.mzp.segmentation_history` as historysource
JOIN (select
      *,
      row_number() over (partition by segmentation_setup_ky order by segmentation_test_end_dt desc) as rn
      from `{{ var.value.INGESTION_PROJECT }}.mzp.segmentation_setup` ) setupsource on (historysource.segmentation_setup_ky=setupsource.segmentation_setup_ky and setupsource.rn=1)
WHERE
  CAST(historysource.adw_lake_insert_datetime AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_mbrs_marketing_segmntn`)
