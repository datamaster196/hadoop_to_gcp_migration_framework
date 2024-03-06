CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_line_work_transformed` AS
SELECT
  COALESCE(policy.ins_policy_adw_key ,
    "-1") AS ins_policy_adw_key ,
  COALESCE(profit.aca_office_adw_key,
    "-1") AS ofc_aca_adw_key ,
  COALESCE(legal_vendor1.vendor_adw_key ,
    "-1") AS legal_issuing_vendor_key ,
  COALESCE(premium_vendor.vendor_adw_key ,
    "-1") AS premium_paid_vendor_key ,
  COALESCE(product1.product_adw_key ,
    "-1") AS product_adw_key ,  
    policy_line_source.uniqline as ins_policy_line_source_key,
   policy_line_source.commissionpercent as ins_line_comm_percent,
   policy_line_source.deliverymethodcode as ins_line_delivery_method_cd, 
   policy_line_source.ins_line_delivery_method_desc,
    policy_line_source.effectivedate  as ins_line_effective_dt ,
  policy_line_source.expirationdate as ins_line_Expiration_dt ,
  policy_line_source.firstwrittendate as ins_line_first_written_dt ,
  linestatus.cdlinestatuscode as ins_line_status,
  policy_line_source.inserteddate as effective_start_datetime,
TO_BASE64(MD5(CONCAT(
ifnull(  COALESCE(policy.ins_policy_adw_key,"-1") ,''), '|',
ifnull(  COALESCE(profit.aca_office_adw_key,"-1") ,''), '|', 
ifnull(  COALESCE(legal_vendor1.vendor_adw_key ,"-1"),''), '|', 
ifnull(  COALESCE(premium_vendor.vendor_adw_key ,"-1") ,''), '|', 
ifnull(  COALESCE(product1.product_adw_key ,"-1") ,''), '|', 
ifnull(policy_line_source.uniqline,''), '|', 
ifnull(policy_line_source.commissionpercent,''), '|', 
ifnull(policy_line_source.deliverymethodcode,''), '|', 
ifnull(policy_line_source.ins_line_delivery_method_desc,''), '|', 
ifnull(policy_line_source.effectivedate,''), '|', 
ifnull(policy_line_source.expirationdate,''), '|', 
ifnull(policy_line_source.firstwrittendate,''), '|', 
ifnull(linestatus.cdlinestatuscode,''), '|'
))) AS adw_row_hash
FROM
(
	(
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_line_work_source` AS policy_line_source
LEFT OUTER JOIN (
  SELECT
    uniqentity,
    lookupcode,
    vendor_cd,
    vendor_adw_key,
    actv_ind,
  FROM (
    SELECT
      company.uniqentity,
      company.lookupcode,
      vendor.vendor_cd,
      vendor.vendor_adw_key,
      vendor.actv_ind,
      ROW_NUMBER() OVER(PARTITION BY company.uniqentity ORDER BY NULL ) AS dupe_check
    FROM
      `{{ var.value.INGESTION_PROJECT }}.epic.company` company,
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor` vendor
    WHERE
      vendor.vendor_cd = company.lookupcode
      AND vendor.actv_ind='Y') legal_vendor
  WHERE
    dupe_check=1 ) legal_vendor1
ON legal_vendor1.uniqentity=policy_line_source.uniqentitycompanyissuing
) 
LEFT OUTER JOIN 
 (
  SELECT
    uniqentity,
    lookupcode,
    vendor_cd,
    vendor_adw_key,
    actv_ind,
  FROM (
    SELECT
      company.uniqentity,
      company.lookupcode,
      vendor.vendor_cd,
      vendor.vendor_adw_key,
      vendor.actv_ind,
      ROW_NUMBER() OVER(PARTITION BY company.uniqentity ORDER BY NULL ) AS dupe_check
    FROM
      `{{ var.value.INGESTION_PROJECT }}.epic.company` company,
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor` vendor
    WHERE
      vendor.vendor_cd = company.lookupcode
      AND vendor.actv_ind='Y') premium_vendor1
  WHERE
    dupe_check=1 ) premium_vendor 
ON premium_vendor.uniqentity=policy_line_source.uniqentitycompanybilling
)  LEFT OUTER JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` policy
ON SAFE_CAST(policy.ins_policy_system_source_key AS STRING)=policy_line_source.uniqpolicy and policy_line_source.policynumber=policy.ins_policy_number
and policy.actv_ind='Y'
LEFT OUTER JOIN (
  SELECT
    profitcenter.uniqprofitcenter,
    profitcenter.profitcentercode,
    office.ins_department_cd,
    office.aca_office_adw_key,
    office.actv_ind,
    ROW_NUMBER() OVER(PARTITION BY profitcenter.uniqprofitcenter ORDER BY NULL ) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.epic.profitcenter` profitcenter,
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` office
  WHERE
    profitcenter.profitcentercode = office.ins_department_cd
    AND office.actv_ind='Y' ) profit
ON
  policy_line_source.uniqprofitcenter=profit.uniqprofitcenter
  AND profit.dupe_check=1
      LEFT OUTER JOIN (
  SELECT
    policylinetype.uniqcdpolicylinetype ,
    policylinetype.cdpolicylinetypecode ,
    product.product_sku ,
    product.product_adw_key ,
    product.actv_ind,
    ROW_NUMBER() OVER(PARTITION BY policylinetype.uniqcdpolicylinetype ORDER BY NULL ) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.epic.cdpolicylinetype` policylinetype,
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` product
  WHERE
    policylinetype.cdpolicylinetypecode = product.product_sku
    AND product.actv_ind='Y' ) product1
ON
  product1.uniqcdpolicylinetype=policy_line_source.uniqcdpolicylinetype
  AND product1.dupe_check=1
  LEFT OUTER JOIN 
  (select uniqcdlinestatus,
          cdlinestatuscode,
          ROW_NUMBER() OVER(PARTITION BY uniqcdlinestatus ORDER BY NULL ) AS dupe_check
    from  `{{ var.value.INGESTION_PROJECT }}.epic.cdlinestatus` ) linestatus
   ON linestatus.uniqcdlinestatus=policy_line_source.uniqcdlinestatus
WHERE
  policy_line_source.dupe_check=1