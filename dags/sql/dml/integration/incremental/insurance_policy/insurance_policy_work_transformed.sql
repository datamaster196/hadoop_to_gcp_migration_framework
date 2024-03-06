CREATE or REPLACE table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_work_transformed` as
SELECT
dim_business_line.biz_line_adw_key,
coalesce((CASE when source.PolicyNumber is not null and (target.ins_policy_number ='' or target.ins_policy_number is null) THEN target.ins_policy_adw_key else '-1' END),'-1')  Ins_quote_adw_key,
'-1' as channel_adw_key,
'-1' as emp_adw_key,
coalesce(work_2.product_category_adw_key,'-1') product_category_adw_key,
coalesce(work_3.state_adw_key,'-1') state_adw_key,
  source.uniqdepartment,
  source.UniqPolicy as ins_policy_system_source_key,
  source.uniqcdpolicylinetype,
  source.UniqAgency,
  AnnualizedCommission as ins_policy_annualized_comm,
  AnnualizedPremium as ins_policy_annualized_premium,
  BilledCommission as ins_policy_billed_comm,
  BilledPremium as ins_policy_billed_premium,
  ContractedExpirationDate as ins_policy_cntrctd_exp_dt,
  EffectiveDate as ins_policy_effective_dt,
  EstimatedCommission as ins_policy_estimated_comm,
  EstimatedPremium as ins_policy_estimated_premium,
  ExpirationDate as ins_policy_Expiration_dt,
  PolicyNumber as ins_policy_number,
source.ins_policy_quote_ind,
TO_BASE64(MD5(CONCAT(ifnull(CAST(dim_business_line.biz_line_adw_key as STRING),''),'|',
ifnull(CAST(-1 as STRING),''),'|',
ifnull(CAST(-1 as STRING),''),'|',
ifnull(CAST(coalesce((CASE when source.PolicyNumber is not null and target.ins_policy_number ='' or target.ins_policy_number is null THEN target.ins_policy_adw_key else '-1' END),'-1') as STRING),''),'|',
ifnull(CAST(work_2.product_category_adw_key as STRING),''),'|',
ifnull(CAST(work_3.state_adw_key as STRING),''),'|',
ifnull(CAST(AnnualizedCommission as STRING),''),'|',
ifnull(CAST(AnnualizedPremium as STRING),''),'|',
ifnull(CAST(BilledCommission as STRING),''),'|',
ifnull(CAST(BilledPremium as STRING),''),'|',
ifnull(CAST(ContractedExpirationDate as STRING),''),'|',
ifnull(CAST(effectiveDate as STRING),''),'|',
ifnull(CAST(EstimatedCommission as STRING),''),'|',
ifnull(CAST(EstimatedPremium as STRING),''),'|',
ifnull(CAST(ExpirationDate as STRING),''),'|',
ifnull(CAST(PolicyNumber as STRING),''),'|',
ifnull(CAST(source.ins_policy_quote_ind as STRING),''),'|'
))) as adw_row_hash,
CAST(source.inserteddate AS datetime) AS effective_start_datetime
,CAST('9999-12-31' AS datetime) effective_end_datetime
,'Y' as actv_ind
,CURRENT_DATETIME() as integrate_insert_datetime
,{{ dag_run.id }} as integrate_insert_batch_number
,CURRENT_DATETIME() as integrate_update_datetime
,{{ dag_run.id }} as integrate_update_batch_number
FROM  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_work_source` as source
left join
(select UniqDepartment,dim_business_line from (select department.UniqDepartment,dim_business_line,
ROW_NUMBER() OVER(PARTITION BY department.UniqDepartment ORDER BY NULL ) AS dupe_check from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_business_line` dim_business_line join
(SELECT  UniqDepartment, CASE
    WHEN DepartmentCode ='CLS' THEN "CL"
    ELSE "PL"
END
  AS DepartmentCode
FROM
  `{{ var.value.INGESTION_PROJECT }}.epic.department`) department
on department.DepartmentCode=dim_business_line.biz_line_cd)  where dupe_check=1) work_1
on work_1.UniqDepartment=source.uniqdepartment
LEFT JOIN
(
select uniqcdpolicylinetype,product_category_adw_key from
(select cdpolicylinetype.uniqcdpolicylinetype,dim_product_category.product_category_adw_key,
ROW_NUMBER() OVER(PARTITION BY cdpolicylinetype.uniqcdpolicylinetype ORDER BY NULL ) AS dupe_check from `{{ var.value.INGESTION_PROJECT }}.epic.cdpolicylinetype` cdpolicylinetype join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` dim_product_category
on dim_product_category.product_category_cd=cdpolicylinetype.typeofbusinesscode
)  where dupe_check=1 ) work_2
on source.uniqcdpolicylinetype = work_2.uniqcdpolicylinetype
left join
(select state_adw_key,UniqAgency from  (select state_adw_key,UniqAgency,
ROW_NUMBER() OVER(PARTITION BY agency.UniqAgency ORDER BY NULL ) AS dupe_check
from `{{ var.value.INGESTION_PROJECT }}.epic.agency`  agency join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_state` dim_state
on  dim_state.state_cd=SUBSTR(agency.AgencyCode,0,2))where dupe_check=1 ) work_3
on work_3.UniqAgency = source.UniqAgency
left outer join (select ins_policy_adw_key, ins_policy_system_source_key,ins_policy_quote_ind, ins_policy_number, row_number() over (partition by ins_policy_system_source_key, ins_policy_quote_ind order by effective_start_datetime asc, effective_end_datetime asc) as dupe_check from `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`) target
on CAST(source.UniqPolicy as INT64)=target.ins_policy_system_source_key and source.ins_policy_quote_ind = target.ins_policy_quote_ind and target.dupe_check=1
where source.dupe_check=1