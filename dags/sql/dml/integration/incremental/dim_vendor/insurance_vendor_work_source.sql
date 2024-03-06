CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_source` AS
  SELECT 
  company_source.uniqcontactaddressaccount,
  company_source.uniqcontactnameprimary,
  company_source.lookupcode as vendor_cd,
  company_source.nameof as vendor_name,
   'Insurance' as vendor_typ,
   CASE WHEN contactname.fullname IS NULL OR contactname.fullname=''
      THEN CONCAT(contactname.firstname,'',contactname.middlename,'',contactname.lastname)
      ELSE contactname.fullname
      END AS vendor_contact,
    CASE WHEN SUBSTR(Nameof,4) = 'CSAA'
    THEN 'Preferred'
    ELSE 'Non-Preferred'
    END as preferred_vendor,
    CASE WHEN inactivedate IS NULL
    THEN 'Active'
    ELSE 'Inactive'
    END as vendor_status,
      COALESCE(company_source.updateddate,company_source.ts) AS updateddate,
         coalesce(TRIM(contactaddress.address1),'') AS address1,
  coalesce(TRIM(contactaddress.address2),'') AS address2,
  coalesce(TRIM(contactaddress.city),
    '') AS city,
  coalesce(TRIM(contactaddress.cdstatecode),
    '') AS cdstatecode,
  coalesce(TRIM(contactaddress.postalcode),
    '') AS postalcode,
  coalesce(TRIM(contactaddress.cdcountrycode),
    '') AS cdcountrycode,
  ROW_NUMBER() OVER(PARTITION BY company_source.lookupcode ORDER BY COALESCE(company_source.updateddate,company_source.ts) DESC) AS DUPE_CHECK
  FROM
  `{{ var.value.INGESTION_PROJECT }}.epic.company` AS company_source
    LEFT JOIN (
    SELECT uniqcontactname,
    firstname,
    middlename,
    lastname,
    fullname,
    ROW_NUMBER() OVER(PARTITION BY uniqcontactname) AS dupe_check
    FROM `{{ var.value.INGESTION_PROJECT }}.epic.contactname`
    ) contactname
    ON contactname.uniqcontactname=company_source.uniqcontactnameprimary AND contactname.dupe_check=1
        LEFT JOIN (select uniqcontactaddress,
                      address1,
                      address2,
                      city,
                      cdstatecode,
                      postalcode,
                      cdcountrycode,
                      ROW_NUMBER() OVER(PARTITION BY uniqcontactaddress) AS dupe_check
    from `{{ var.value.INGESTION_PROJECT }}.epic.contactaddress`        
    ) contactaddress 
    ON contactaddress.uniqcontactaddress=company_source.uniqcontactaddressaccount
    WHERE
  CAST(COALESCE(company_source.updateddate,company_source.ts) AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor`)