CREATE OR REPLACE TABLE
`{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_line_work_source` AS
SELECT
policy_line_source.uniqline ,
policy_line_source.uniqpolicy ,
policy_line_source.uniqprofitcenter ,
policy_line_source.uniqentitycompanyissuing ,
policy_line_source.uniqentitycompanybilling ,
policy_line_source.uniqcdpolicylinetype ,
policy_line_source.commissionpercent ,
policy_line_source.deliverymethodcode ,
    CASE WHEN policy_line_source.deliverymethodcode ='E' THEN 'EMAIL'
     WHEN policy_line_source.deliverymethodcode ='M' THEN 'MAIL'
     WHEN policy_line_source.deliverymethodcode ='F' THEN 'FAX'
    ELSE 'OTHER' END as ins_line_delivery_method_desc,
policy_line_source.effectivedate ,
policy_line_source.expirationdate ,
policy_line_source.firstwrittendate ,
policy_line_source.uniqcdlinestatus ,
coalesce(policy_line_source.updateddate,policy_line_source.ts,policy_line_source.inserteddate) as inserteddate,
lake_policy.policynumber,
  ROW_NUMBER() OVER(PARTITION BY policy_line_source.uniqline,coalesce(policy_line_source.updateddate,policy_line_source.ts,policy_line_source.inserteddate)  ORDER BY null ) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.epic.line` AS policy_line_source

   LEFT OUTER join  `{{ var.value.INGESTION_PROJECT }}.epic.policy` lake_policy
   ON lake_policy.uniqpolicy =policy_line_source.uniqpolicy