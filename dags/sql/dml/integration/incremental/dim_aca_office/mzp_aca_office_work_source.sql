CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.office_source` AS
SELECT
  Address AS Address,
  State AS State,
  Phone AS Phone,
  Code AS Code,
  Name AS Name,
  Status AS Status,
  Officetype AS Officetype,
  branch1.DIVISION_KY AS mbrs_division_ky,
  division.DIVISION_NAME AS mbrs_division_nm,
  MembershipOfficeID AS MembershipOfficeID,
  InsuranceOfficeID AS InsuranceOfficeID,
  CommCenter AS CommCenter,
  CarCareOfficeID AS CarCareOfficeID,
  TravelOfficeID AS TravelOfficeID,
  SAFE_CAST(NULL AS STRING) AS Division,
  RetailSalesOfficeID AS RetailSalesOfficeID,
  RM_Cost_Center_Manager AS RM_Cost_Center_Manager,
  RM_Cost_Center_Manager_Email AS RM_Cost_Center_Manager_Email,
  RAM AS RAM,
  RAM_Email AS RAM_Email,
  Staff_Assistant AS Staff_Assistant,
  Staff_Assistant_Email AS Staff_Assistant_Email,
  Car_Care_Manager_Cost_Center_Manager AS Car_Care_Manager_Cost_Center_Manager,
  Care_Care_Manager_Cost_Center_Manager_Email AS Care_Care_Manager_Cost_Center_Manager_Email,
  Car_Care_Asst_Manager AS Car_Care_Asst_Manager,
  Car_Care_Asst_Manager_Email AS Car_Care_Asst_Manager_Email,
  RTSM AS RTSM,
  RTSM_Email AS RTSM_Email,
  Retail_Sales_Coach AS Retail_Sales_Coach,
  Retail_Sales_Coach_email AS Retail_Sales_Coach_email,
  Xactly_Retail_Store_Number AS Xactly_Retail_Store_Number,
  Xactly_Care_Care_Store_Number AS Xactly_Care_Care_Store_Number,
  PresidentRegion AS PresidentRegion,
  GM_Car_Care AS GM_Car_Care,
  GM_Car_Care_Email AS GM_Car_Care_Email,
  Car_Care_Director AS Car_Care_Director,
  Car_Care_Director_email AS Car_Care_Director_email,
  CarCareDistrictNumber AS CarCareDistrictNumber,
  CarCareCostCenter AS CarCareCostCenter,
  TireCostCenter AS TireCostCenter,
  AdminCostCenter AS AdminCostCenter,
  OccupancyCostCenter AS OccupancyCostCenter,
  GM_Retail AS GM_Retail,
  GM_Retail_Email AS GM_Retail_Email,
  Director_District_Manager AS Director_District_Manager,
  Director_District_Manager_Email AS Director_District_Manager_Email,
  RetailDistrictNumber AS RetailDistrictNumber,
  RetailSalesCostCenter AS RetailSalesCostCenter,
  TravelCostCenter AS TravelCostCenter,
  MembershipCostCenter AS MembershipCostCenter,
  FacilityCostCenter AS FacilityCostCenter,
  National_ATS_Number AS National_ATS_Number,
  ROW_NUMBER() OVER (PARTITION BY Code ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.aca_store_list` source
    LEFT OUTER JOIN 
   (
  SELECT
    BRANCH_CD,
      DIVISION_KY ,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_CD ORDER BY NULL ) AS dupe_check
  FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.branch` branch
  where CLUB_CD='212'
 ) branch1
  ON source.MembershipOfficeID = branch1.BRANCH_CD
  AND branch1.dupe_check=1
  LEFT OUTER JOIN 
   (SELECT
    DIVISION_KY ,
      DIVISION_NAME ,
    ROW_NUMBER() OVER(PARTITION BY DIVISION_KY ORDER BY NULL ) AS dupe_check
  FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.division`) division
  ON branch1.DIVISION_KY=division.DIVISION_KY
  AND division.dupe_check=1
WHERE
  CAST(CURRENT_DATE() AS datetime) > (
  SELECT
    COALESCE(MAX(effective_start_datetime),
      SAFE_CAST('1900-01-01' AS datetime))
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`)