CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_work_source` AS
SELECT
  uniqdepartment,
  UniqPolicy,
  uniqcdpolicylinetype,
  UniqAgency,
  AnnualizedCommission,
  AnnualizedPremium,
  BilledCommission,
  BilledPremium,
  ContractedExpirationDate,
  EffectiveDate,
  EstimatedCommission,
  EstimatedPremium,
  ExpirationDate,
  PolicyNumber,
   CASE
 WHEN
PolicyNumber ='' or PolicyNumber is null then 'Y' else 'N' END ins_policy_quote_ind,
  coalesce(updateddate,ts,inserteddate) as inserteddate,
   ROW_NUMBER() OVER(PARTITION BY source.UniqPolicy ORDER BY coalesce(updateddate,ts,inserteddate) DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.epic.policy`  AS source
 WHERE
  CAST(coalesce(updateddate,ts,inserteddate) as DATETIME)  > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`)