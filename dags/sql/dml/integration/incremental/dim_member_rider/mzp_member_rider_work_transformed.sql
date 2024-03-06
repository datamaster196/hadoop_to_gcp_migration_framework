CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_transformed` AS
SELECT
coalesce(mbr_adw_key,'-1') as mbr_adw_key,
coalesce(membership.mbrs_adw_key,'-1') as mbrs_adw_key,
coalesce(product.product_adw_key,'-1') as product_adw_key,
coalesce(employee_role.emp_role_adw_key,'-1') as emp_role_adw_key,
mbr_rider_status_dtm
,mbr_rider_status_cd
,mbr_rider_effective_dt
,mbr_rider_cancel_dt
,mbr_rider_cancel_reason_cd
,codes.code_desc as mbr_rider_cancel_reason_desc
,mbr_rider_billing_category_cd
,mbr_rider_solicit_cd
,mbr_rider_cost_effective_dt
,mbr_rider_actual_cancel_dtm
,coalesce(auto_renewal.mbr_ar_card_adw_key,'-1') as mbr_ar_card_adw_key
,mbr_rider_source_key
,mbr_rider_do_not_renew_ind
,mbr_rider_dues_cost_amt
,mbr_rider_dues_adj_amt
,mbr_rider_payment_amt
,mbr_rider_paid_by_cd
,mbr_rider_future_cancel_dt
,mbr_rider_reinstate_ind
,mbr_rider_original_cost_amt
,mbr_rider_reinstate_reason_cd
,mbr_rider_extend_exp_amt
,mbr_rider_def_key
,mbr_rider_actvtn_dt
,CAST (rider.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(COALESCE(mbr_rider_status_dtm,'')
,COALESCE(mbr_rider_status_cd,'')
,COALESCE(mbr_rider_effective_dt,'')
,COALESCE(mbr_rider_cancel_dt,'')
,COALESCE(mbr_rider_cancel_reason_cd,'')
,COALESCE(mbr_rider_billing_category_cd,'')
,COALESCE(mbr_rider_solicit_cd,'')
,COALESCE(mbr_rider_cost_effective_dt,'')
,COALESCE(mbr_rider_actual_cancel_dtm,'')
,COALESCE(mbr_rider_source_key,'')
,COALESCE(mbr_rider_do_not_renew_ind,'')
,COALESCE(mbr_rider_dues_cost_amt,'')
,COALESCE(mbr_rider_dues_adj_amt,'')
,COALESCE(mbr_rider_payment_amt,'')
,COALESCE(mbr_rider_paid_by_cd,'')
,COALESCE(mbr_rider_future_cancel_dt,'')
,COALESCE(mbr_rider_reinstate_ind,'')
,COALESCE(mbr_rider_original_cost_amt,'')
,COALESCE(mbr_rider_reinstate_reason_cd,'')
,COALESCE(mbr_rider_extend_exp_amt,'')
,COALESCE(mbr_rider_def_key,'')
,COALESCE(mbr_rider_actvtn_dt,'')
,COALESCE(mbr_adw_key,'-1')
,COALESCE(product.product_adw_key,'-1')
,COALESCE(auto_renewal.mbr_ar_card_adw_key,'-1')
,COALESCE(employee_role.emp_role_adw_key,'-1')
,COALESCE(membership.mbrs_adw_key,'-1')
))) as adw_row_hash
,
 ROW_NUMBER() OVER (PARTITION BY mbr_rider_source_key ORDER BY last_upd_dt DESC ) AS DUP_CHECK
FROM
 `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_source` AS rider
left  join
`{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` member
on
SAFE_CAST(rider.member_ky as INT64)=member.mbr_source_system_key and member.actv_ind='Y'
left  join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` membership on SAFE_CAST(rider.membership_ky as INT64) =membership.mbrs_source_system_key and membership.actv_ind='Y'
left  join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role` employee_role on employee_role.emp_role_id=rider.agent_id and employee_role.actv_ind='Y'
left join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` auto_renewal on auto_renewal.mbr_source_arc_key=CAST(rider.AUTORENEWAL_CARD_KY as INT64) and auto_renewal.actv_ind = 'Y'
left  join (
    select
        product.*
    from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` product
    join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` dim_product_category ON
        product_category_cd IN  ('RCC','MBR_FEES')
        and dim_product_category.product_category_adw_key =product.product_category_adw_key
        ) product on rider.rider_comp_cd=product.product_sku_key and product.actv_ind='Y'
left join  (select distinct code, code_desc, ROW_NUMBER() OVER (PARTITION BY code ORDER BY cast(last_upd_dt as datetime) DESC ) AS DUP_CHECK  from `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes` where code_type = 'CANRES') as codes
    on rider.mbr_rider_cancel_reason_cd = codes.code and codes.DUP_CHECK = 1
where rider.dup_check=1