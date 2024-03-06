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
  coalesce(updateddate,ts,inserteddate) as inserteddate,
   CASE
 WHEN
PolicyNumber ='' or PolicyNumber is null then 'Y' else 'N' END insurance_policy_quote_indicator,
   ROW_NUMBER() OVER(PARTITION BY source.UniqPolicy, coalesce(updateddate,ts,inserteddate) ORDER BY coalesce(updateddate,ts,inserteddate) DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.epic.policy`  AS source