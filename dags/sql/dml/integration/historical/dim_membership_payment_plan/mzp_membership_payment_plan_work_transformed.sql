CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_transformed` as 
SELECT 
 coalesce(adw_memship.mbrs_adw_key,'-1') as mbrs_adw_key,
 coalesce(adw_rider.mbr_rider_adw_key,'-1') as mbr_rider_adw_key,
 cast(PLAN_BILLING_KY as int64) as payment_plan_source_key ,
 source.PLAN_NAME as payment_plan_nm,
 cast(source.NUMBER_OF_PAYMENTS as int64) as payment_plan_payment_cnt,
 cast(source.PLAN_LENGTH as int64) as payment_plan_month,
 cast(source.PAYMENT_NUMBER as int64) as payment_nbr,
 source.PAYMENT_STATUS as payment_plan_status,
 cast(cast(source.CHARGE_DT as datetime) as date) as payment_plan_charge_dt,
 case when source.DONATION_HISTORY_KY is null then 'N' else 'Y' end as safety_fund_donation_ind ,
 cast(source.PAYMENT_AT as numeric) as payment_amt,
 CAST(source.last_upd_dt AS datetime) AS last_upd_dt,
 CAST('9999-12-31' AS datetime) effective_end_datetime,
 'Y' as actv_ind,
 TO_BASE64(MD5(CONCAT(
            ifnull(source.PLAN_BILLING_KY,''),'|',
            ifnull(source.membership_ky,''),'|',
            ifnull(source.rider_ky,''),'|',
            ifnull(source.PLAN_NAME,''),'|',
            ifnull(source.NUMBER_OF_PAYMENTS,''),'|',
            ifnull(source.PLAN_LENGTH,''),'|',
            ifnull(source.PAYMENT_NUMBER,''),'|',
            ifnull(source.PAYMENT_STATUS,''),'|',
            ifnull(source.CHARGE_DT,''),'|',
            ifnull(source.DONATION_HISTORY_KY,''),'|',
            ifnull(source.PAYMENT_AT,'')
            ))) as adw_row_hash,
 CURRENT_DATETIME() as integrate_insert_datetime,
 {{ dag_run.id }} as integrate_insert_batch_number,
 CURRENT_DATETIME() as integrate_update_datetime,
 {{ dag_run.id }} as integrate_update_batch_number
FROM
`{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_source` as source 
 LEFT JOIN `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` adw_memship
 on source.membership_ky =SAFE_CAST(adw_memship.mbrs_source_system_key AS STRING) AND adw_memship.actv_ind='Y'
 LEFT JOIN `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider` adw_rider
 on source.rider_ky =SAFE_CAST(adw_rider.mbr_rider_source_key AS STRING) AND adw_rider.actv_ind='Y'
WHERE source.dupe_check=1 
 