from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "membership_payment_applied_detail"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22),
    'email': ['airflow@example.com'],
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

####################################################################################################################################
# Product
####################################################################################################################################

product_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source`  as 
SELECT 
  code_type
  , code
  , code_desc
  , last_upd_dt
  , ROW_NUMBER() OVER(PARTITION BY source.code_ky ORDER BY source.last_upd_dt DESC) AS dupe_check
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` as source 
WHERE code_type in  ( 'FEETYP', 'COMPCD')  and 
  CAST(source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_product`)


"""
product_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` as 
SELECT 
  CASE code_type
    WHEN 'FEETYP' THEN (SELECT product_category_adw_key  from  `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` where product_category_code  = 'MBR_FEES')  
    WHEN 'COMPCD' THEN (SELECT product_category_adw_key  from  `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` where product_category_code  = 'RCC' )
  END AS product_category_adw_key  
  , concat(ifnull(code_type,''),'|', ifnull(code,'') ) as product_sku 
  , code as product_sku_key
  ,'-1' as vendor_adw_key
  , code_desc as product_sku_description
  , SAFE_CAST(last_upd_dt AS DATETIME)  as effective_start_datetime
  , CAST('9999-12-31' AS datetime) effective_end_datetime
  , 'Y' as active_indicator
  , CURRENT_DATETIME() as integrate_insert_datetime
  , 1 as integrate_insert_batch_number
  , CURRENT_DATETIME() as integrate_update_datetime
  , 1 as integrate_update_batch_number
  , TO_BASE64(MD5(CONCAT(COALESCE(code_type,''),COALESCE(code_desc,'')))) as adw_row_hash 
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source` as source 
WHERE source.dupe_check=1 
"""

product_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source_stage` AS
SELECT
  GENERATE_UUID() as product_adw_key, 
  source.product_category_adw_key,
  source.product_sku,
  source.product_sku_key,
  source.vendor_adw_key,
  source.product_sku_description,
  source.adw_row_hash,
  source.effective_start_datetime,
  source.effective_end_datetime,
  source.active_indicator,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku
    AND target.active_indicator='Y')
WHERE
  target.product_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
  target.product_adw_key,
  target.product_category_adw_key,
  target.product_sku,
  target.product_sku_key,
  target.vendor_adw_key,
  target.product_sku_description,
  target.adw_row_hash,
  target.effective_start_datetime,
  DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 day) AS effective_end_datetime,
  'N' AS active_indicator,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_product` target
ON
  (source.product_sku=target.product_sku
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""

product_merge = f"""
MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.adw.dim_product` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_cx_codes_source_stage` b
ON
  (a.product_adw_key = b.product_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  product_adw_key ,    
  product_category_adw_key,
  product_sku,
  product_sku_key,  
  vendor_adw_key,  
  product_sku_description,
  effective_start_datetime,
  effective_end_datetime,
  active_indicator,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number,
  adw_row_hash  
  ) VALUES (
  b.product_adw_key ,    
  b.product_category_adw_key,
  b.product_sku,
  b.product_sku_key,
  b.vendor_adw_key,    
  b.product_sku_description,
  b.effective_start_datetime,
  b.effective_end_datetime,
  b.active_indicator,
  b.integrate_insert_datetime,
  b.integrate_insert_batch_number,
  b.integrate_update_datetime,
  b.integrate_update_batch_number,
  b.adw_row_hash)
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number
"""

######################################################################
# Load office Current set of Data
######################################################################
# Queries

membership_office_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE `{int_u.INTEGRATION_PROJECT}.adw_work.office_source`  AS
SELECT
Address as Address,
State as State,
Phone as Phone,
Code as Code,
Name as Name,
Status as Status,
Officetype as Officetype,
MembershipOfficeID  AS MembershipOfficeID ,
InsuranceOfficeID AS InsuranceOfficeID,
CommCenter AS CommCenter,
CarCareOfficeID AS CarCareOfficeID,
TravelOfficeID AS TravelOfficeID,
SAFE_CAST(NULL AS STRING) as Division,
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
CarCareRegion AS CarCareRegion,
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
ROW_NUMBER() OVER (PARTITION BY Code ORDER BY CURRENT_DATE() DESC) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.aca_store_list` source
  WHERE
  CAST(CURRENT_DATE() AS datetime) > (
  SELECT
    COALESCE(MAX(effective_start_datetime),'1900-01-01 12:00:00')
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office`)
"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_office_work_transformed = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_office_work_transformed` AS
  SELECT 
  Address,
State,
Phone,
Code,
Name,
Status,
Officetype,
MembershipOfficeID,
InsuranceOfficeID,
CommCenter,
CarCareOfficeID,
TravelOfficeID,
Division,
RetailSalesOfficeID,
RM_Cost_Center_Manager,
RM_Cost_Center_Manager_Email,
RAM,
RAM_Email,
Staff_Assistant,
Staff_Assistant_Email,
Car_Care_Manager_Cost_Center_Manager,
Care_Care_Manager_Cost_Center_Manager_Email,
Car_Care_Asst_Manager,
Car_Care_Asst_Manager_Email,
RTSM,
RTSM_Email,
Retail_Sales_Coach,
Retail_Sales_Coach_email,
Xactly_Retail_Store_Number,
Xactly_Care_Care_Store_Number,
CarCareRegion,
GM_Car_Care,
GM_Car_Care_Email,
Car_Care_Director,
Car_Care_Director_email,
CarCareDistrictNumber,
CarCareCostCenter,
TireCostCenter,
AdminCostCenter,
OccupancyCostCenter,
GM_Retail,
GM_Retail_Email,
Director_District_Manager,
Director_District_Manager_Email,
RetailDistrictNumber,
RetailSalesCostCenter,
TravelCostCenter,
MembershipCostCenter,
FacilityCostCenter,
National_ATS_Number,
dupe_check,
CURRENT_DATETIME() AS last_upd_dt,
TO_BASE64(MD5(CONCAT(
ifnull(Address,''),'|',
ifnull(State,''),'|',
ifnull(Phone,''),'|',
ifnull(Name,''),'|',
ifnull(Status,''),'|',
ifnull(Officetype,''),'|',
ifnull(MembershipOfficeID,''),'|',
ifnull(InsuranceOfficeID,''),'|',
ifnull(CommCenter,''),'|',
ifnull(CarCareOfficeID,''),'|',
ifnull(TravelOfficeID,''),'|',
ifnull(Division,''),'|',
ifnull(RetailSalesOfficeID,''),'|',
ifnull(RM_Cost_Center_Manager,''),'|',
ifnull(RM_Cost_Center_Manager_Email,''),'|',
ifnull(RAM,''),'|',
ifnull(RAM_Email,''),'|',
ifnull(Staff_Assistant,''),'|',
ifnull(Staff_Assistant_Email,''),'|',
ifnull(Car_Care_Manager_Cost_Center_Manager,''),'|',
ifnull(Care_Care_Manager_Cost_Center_Manager_Email,''),'|',
ifnull(Car_Care_Asst_Manager,''),'|',
ifnull(Car_Care_Asst_Manager_Email,''),'|',
ifnull(RTSM,''),'|',
ifnull(RTSM_Email,''),'|',
ifnull(Retail_Sales_Coach,''),'|',
ifnull(Retail_Sales_Coach_email,''),'|',
ifnull(Xactly_Retail_Store_Number,''),'|',
ifnull(Xactly_Care_Care_Store_Number,''),'|',
ifnull(CarCareRegion,''),'|',
ifnull(GM_Car_Care,''),'|',
ifnull(GM_Car_Care_Email,''),'|',
ifnull(Car_Care_Director,''),'|',
ifnull(Car_Care_Director_email,''),'|',
ifnull(CarCareDistrictNumber,''),'|',
ifnull(CarCareCostCenter,''),'|',
ifnull(TireCostCenter,''),'|',
ifnull(AdminCostCenter,''),'|',
ifnull(OccupancyCostCenter,''),'|',
ifnull(GM_Retail,''),'|',
ifnull(GM_Retail_Email,''),'|',
ifnull(Director_District_Manager,''),'|',
ifnull(Director_District_Manager_Email,''),'|',
ifnull(RetailDistrictNumber,''),'|',
ifnull(RetailSalesCostCenter,''),'|',
ifnull(TravelCostCenter,''),'|',
ifnull(MembershipCostCenter,''),'|',
ifnull(FacilityCostCenter,''),'|',
ifnull(National_ATS_Number,''),'|'
))) AS adw_row_hash
  FROM `{int_u.INTEGRATION_PROJECT}.adw_work.office_source` AS office
  WHERE dupe_check =1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_office_work_final_staging = f"""

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.office_work_stage` AS
  SELECT
    COALESCE(target.aca_office_adw_key ,GENERATE_UUID()) as aca_office_adw_key,
source.Address as aca_office_address,
source.State as aca__office_state_code ,
source.Phone as aca_office_phone_number ,
SAFE_CAST(source.Code AS INT64) as aca_office_list_source_key,
source.Name as aca_office_name,
source.Status as aca_office_status,
source.Officetype as aca_office_type,
source.MembershipOfficeID as membership_branch_code,
source.InsuranceOfficeID as insurance_department_code,
source.CommCenter as roadside_assistance_communicator_center,
source.CarCareOfficeID as car_care_location_number,
source.TravelOfficeID as travel_branch_code,
source.Division as aca_store_division,
source.RetailSalesOfficeID as retail_sales_office_code,
source.RM_Cost_Center_Manager as retail_manager,
source.RM_Cost_Center_Manager_Email as retail_manager_email,
source.RAM as retail_assistant_manager,
source.RAM_Email as retail_assistant_manager_email,
source.Staff_Assistant as staff_assistant,
source.Staff_Assistant_Email as staff_assistant_email,
source.Car_Care_Manager_Cost_Center_Manager as car_care_manager,
source.Care_Care_Manager_Cost_Center_Manager_Email as car_care_manager_email,
source.Car_Care_Asst_Manager as car_care_assistant_manager,
source.Car_Care_Asst_Manager_Email as car_care_assistant_manager_email,
source.RTSM as regional_travel_sales_manager,
source.RTSM_Email as regional_travel_sales_manager_email,
source.Retail_Sales_Coach as retail_sales_coach,
source.Retail_Sales_Coach_email as retail_sales_coach_email,
source.Xactly_Retail_Store_Number as xactly_retail_store_number,
source.Xactly_Care_Care_Store_Number as xactly_car_care_store_number,
source.CarCareRegion as region,
source.GM_Car_Care as car_care_general_manager,
source.GM_Car_Care_Email as car_care_general_manager_email,
source.Car_Care_Director as car_care_director,
source.Car_Care_Director_email as car_care_director_email,
source.CarCareDistrictNumber as car_care_district_number,
source.CarCareCostCenter as car_care_cost_center_number,
source.TireCostCenter as tire_cost_center_number,
source.AdminCostCenter as admin_cost_center_number,
source.OccupancyCostCenter as occupancy_cost_center_number,
source.GM_Retail as retail_general_manager,
source.GM_Retail_Email as retail_general_manager_email,
source.Director_District_Manager as district_manager,
source.Director_District_Manager_Email as district_manager_email,
source.RetailDistrictNumber as retail_district_number,
source.RetailSalesCostCenter as retail_cost_center_number,
source.TravelCostCenter as travel_cost_center_number,
source.MembershipCostCenter as membership_cost_center_number,
source.FacilityCostCenter as facility_cost_center_number,
source.National_ATS_Number as national_ats_number,
  last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  1 integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  1 integrate_update_batch_number
    FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_office_work_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` target
ON
  (CAST(source.code AS String)=CAST(target.aca_office_list_source_key AS String)
    AND target.active_indicator='Y')
WHERE
  target.aca_office_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
  UNION ALL
  SELECT
  target.aca_office_adw_key,
target.aca_office_address,
target.aca__office_state_code,
target.aca_office_phone_number ,
target.aca_office_list_source_key,
target.aca_office_name,
target.aca_office_status,
target.aca_office_type,
target.membership_branch_code,
target.insurance_department_code,
target.roadside_assistance_communicator_center,
target.car_care_location_number,
target.travel_branch_code,
target.aca_office_division ,
target.retail_sales_office_code,
target.retail_manager,
target.retail_manager_email,
target.retail_assistant_manager,
target.retail_assistant_manager_email,
target.staff_assistant,
target.staff_assistant_email,
target.car_care_manager,
target.car_care_manager_email,
target.car_care_assistant_manager,
target.car_care_assistant_manager_email,
target.regional_travel_sales_manager,
target.regional_travel_sales_manager_email,
target.retail_sales_coach,
target.retail_sales_coach_email,
target.xactly_retail_store_number,
target.xactly_car_care_store_number,
target.region,
target.car_care_general_manager,
target.car_care_general_manager_email,
target.car_care_director,
target.car_care_director_email,
target.car_care_district_number,
target.car_care_cost_center_number,
target.tire_cost_center_number,
target.admin_cost_center_number,
target.occupancy_cost_center_number,
target.retail_general_manager,
target.retail_general_manager_email,
target.district_manager,
target.district_manager_email,
target.retail_district_number,
target.retail_cost_center_number,
target.travel_cost_center_number,
target.membership_cost_center_number,
target.facility_cost_center_number,
target.national_ats_number,
  target.effective_start_datetime,
  DATETIME_SUB(source.last_upd_dt, INTERVAL 1 SECOND) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_office_work_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` target
ON
  (CAST(source.code AS String)=CAST(target.aca_office_list_source_key AS String)
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash


"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_office
membership_office_merge = f"""

MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.office_work_stage` b
ON a.aca_office_adw_key=b.aca_office_adw_key
when not matched then INSERT
(
aca_office_adw_key,
aca_office_address,
aca__office_state_code ,
aca_office_phone_number ,
aca_office_list_source_key,
aca_office_name,
aca_office_status,
aca_office_type,
membership_branch_code,
insurance_department_code,
roadside_assistance_communicator_center,
car_care_location_number,
travel_branch_code,
aca_office_division,
retail_sales_office_code,
retail_manager,
retail_manager_email,
retail_assistant_manager,
retail_assistant_manager_email,
staff_assistant,
staff_assistant_email,
car_care_manager,
car_care_manager_email,
car_care_assistant_manager,
car_care_assistant_manager_email,
regional_travel_sales_manager,
regional_travel_sales_manager_email,
retail_sales_coach,
retail_sales_coach_email,
xactly_retail_store_number,
xactly_car_care_store_number,
region,
car_care_general_manager,
car_care_general_manager_email,
car_care_director,
car_care_director_email,
car_care_district_number,
car_care_cost_center_number,
tire_cost_center_number,
admin_cost_center_number,
occupancy_cost_center_number,
retail_general_manager,
retail_general_manager_email,
district_manager,
district_manager_email,
retail_district_number,
retail_cost_center_number,
travel_cost_center_number,
membership_cost_center_number,
facility_cost_center_number,
national_ats_number,
effective_start_datetime,
effective_end_datetime,
active_indicator,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number,
adw_row_hash
) VALUES (
b.aca_office_adw_key ,
b.aca_office_address,
b.aca__office_state_code,
b.aca_office_phone_number,
b.aca_office_list_source_key,
b.aca_office_name,
b.aca_office_status,
b.aca_office_type,
b.membership_branch_code,
b.insurance_department_code,
b.roadside_assistance_communicator_center,
b.car_care_location_number,
b.travel_branch_code,
b.aca_store_division,
b.retail_sales_office_code,
b.retail_manager,
b.retail_manager_email,
b.retail_assistant_manager,
b.retail_assistant_manager_email,
b.staff_assistant,
b.staff_assistant_email,
b.car_care_manager,
b.car_care_manager_email,
b.car_care_assistant_manager,
b.car_care_assistant_manager_email,
b.regional_travel_sales_manager,
b.regional_travel_sales_manager_email,
b.retail_sales_coach,
b.retail_sales_coach_email,
b.xactly_retail_store_number,
b.xactly_car_care_store_number,
b.region,
b.car_care_general_manager,
b.car_care_general_manager_email,
b.car_care_director,
b.car_care_director_email,
b.car_care_district_number,
b.car_care_cost_center_number,
b.tire_cost_center_number,
b.admin_cost_center_number,
b.occupancy_cost_center_number,
b.retail_general_manager,
b.retail_general_manager_email,
b.district_manager,
b.district_manager_email,
b.retail_district_number,
b.retail_cost_center_number,
b.travel_cost_center_number,
b.membership_cost_center_number,
b.facility_cost_center_number,
b.national_ats_number,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number,
b.adw_row_hash 
)
WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number 
  """

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_source` AS
SELECT
  membership_source.billing_cd,
  membership_source.membership_id,
  membership_source.membership_ky,
  membership_source.address_line1,
  membership_source.address_line2,
  membership_source.city,
  membership_source.state,
  membership_source.zip,
  membership_source.country,
  membership_source.membership_type_cd,
  membership_source.cancel_dt,
  membership_source.status_dt,
  membership_source.phone,
  membership_source.status,
  membership_source.billing_category_cd,
  membership_source.dues_cost_at,
  membership_source.dues_adjustment_at,
  membership_source.future_cancel_fl,
  membership_source.future_cancel_dt,
  membership_source.oot_fl,
  membership_source.address_change_dt,
  membership_source.adm_d2000_update_dt,
  membership_source.tier_ky,
  membership_source.temp_addr_ind,
  membership_source.previous_membership_id,
  membership_source.previous_club_cd,
  membership_source.transfer_club_cd,
  membership_source.transfer_dt,
  membership_source.dont_solicit_fl,
  membership_source.bad_address_fl,
  membership_source.dont_send_pub_fl,
  membership_source.market_tracking_cd,
  membership_source.salvage_fl,
  membership_source.coverage_level_cd,
  membership_source.group_cd,
--  membership_source.coverage_level_ky,
  membership_source.ebill_fl,
  membership_source.segmentation_cd,
  membership_source.promo_code,
  membership_source.phone_type,
  membership_source.branch_ky,  
  membership_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY membership_source.membership_id ORDER BY membership_source.last_upd_dt DESC) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.membership` AS membership_source
WHERE
  CAST(membership_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` AS
  SELECT
    membership.billing_cd AS membership_billing_code,
    membership.membership_id AS membership_identifier,
    membership.membership_ky AS membership_source_system_key,
    TO_BASE64(MD5(CONCAT(ifnull(membership.address_line1,
            ''),'|',ifnull(membership.address_line2,
            ''),'|',ifnull(membership.city,
            ''),'|',ifnull(membership.state,
            ''),'|',ifnull(membership.zip,
            ''),'|',ifnull(membership.country,
            '')))) AS address_adw_key,
    membership.membership_type_cd AS membership_type_code,
    membership.cancel_dt AS membership_cancel_date,
    membership.status_dt AS membership_status_datetime,
    coalesce(phone.phone_adw_key, '-1') AS phone_adw_key,
    coalesce(product.product_adw_key, '-1') AS product_adw_key,		
    coalesce(aca_office.aca_office_adw_key, '-1') as aca_office_adw_key,
    membership.status AS membership_status_code,
    membership.billing_category_cd AS membership_billing_category_code,
    'N' AS membership_purge_indicator,
    membership.dues_cost_at AS membership_dues_cost_amount,
    membership.dues_adjustment_at AS membership_dues_adjustment_amount,
    membership.future_cancel_fl AS membership_future_cancel_indicator,
    membership.future_cancel_dt AS membership_future_cancel_date,
    membership.oot_fl AS membership_out_of_territory_code,
    membership.address_change_dt AS membership_address_change_datetime,
    membership.adm_d2000_update_dt AS membership_cdx_export_update_datetime,
    membership.tier_ky AS membership_tier_key,
    membership.temp_addr_ind AS membership_temporary_address_indicator,
    membership.previous_membership_id AS previous_club_membership_identifier,
    membership.previous_club_cd AS previous_club_code,
    membership.transfer_club_cd AS membership_transfer_club_code,
    membership.transfer_dt AS membership_transfer_datetime,
    membership_crossref.previous_club_cd as merged_membership_previous_club_code,
    membership_crossref.previous_membership_id as merged_member_previous_membership_identifier,
    membership_crossref.previous_membership_ky as merged_member_previous_membership_key,
    membership.dont_solicit_fl AS membership_dn_solicit_indicator,
    membership.bad_address_fl AS membership_bad_address_indicator,
    membership.dont_send_pub_fl AS membership_dn_send_publications_indicator,
    membership.market_tracking_cd AS membership_market_tracking_code,
    membership.salvage_fl AS membership_salvage_flag,
    membership.coverage_level_cd AS membership_coverage_level_code,
    membership.group_cd AS membership_group_code,
 --   membership.coverage_level_ky AS membership_coverage_level_key,
    membership.ebill_fl AS membership_ebill_indicator,
    membership.segmentation_cd AS membership_marketing_segmentation_code,
    membership.promo_code AS membership_promo_code,
    membership.phone_type AS membership_phone_type_code,
    membership_code.ERS_CODE AS membership_ers_abuser_indicator,
    membership_code.DSP_CODE AS membership_dn_send_aaaworld_indicator,
    membership_code.SAL_CODE AS membership_salvaged_indicator,
    membership_code.SHMC_CODE AS membership_shadow_motorcycle_indicator,
    membership_code.STDNT_CODE AS membership_student_indicator,
    membership_code.NSAL_CODE AS membership_dn_salvage_indicator,
    membership_code.NCOA_CODE AS membership_address_updated_by_ncoa_indicator,
    membership_code.RET_CODE AS membership_assigned_retention_indicator,
    membership_code.NMSP_CODE AS membership_no_membership_promotion_mailings_indicator,
    membership_code.DNTM_CODE AS membership_dn_telemarket_indicator,
    membership_code.NINETY_CODE AS membership_dn_offer_plus_indicator,
    membership_code.AAAWON_CODE AS membership_receive_aaaworld_online_edition_indicator,
    membership_code.SHADOW_CODE AS membership_shadowrv_coverage_indicator,
    membership_code.DNDM_CODE AS membership_dn_direct_mail_solicit_indicator,
    membership_code.SIX_CODE AS membership_heavy_ers_indicator,
    CAST (membership.last_upd_dt AS datetime) AS last_upd_dt,
    TO_BASE64(MD5(CONCAT(ifnull(membership.billing_cd,
            ''),'|',ifnull(membership.membership_id,
            ''),'|',ifnull(membership.address_line1,
            ''),'|',ifnull(membership.address_line2,
            ''),'|',ifnull(membership.city,
            ''),'|',ifnull(membership.state,
            ''),'|',ifnull(membership.zip,
            ''),'|',ifnull(membership.country,
            ''),'|',ifnull(membership.membership_type_cd,
            ''),'|',ifnull(membership.cancel_dt,
            ''),'|',ifnull(membership.status_dt,
            ''),'|',ifnull(membership.status,
            ''),'|',ifnull(membership.billing_category_cd,
            ''),'|',ifnull(membership.dues_cost_at,
            ''),'|',ifnull(membership.dues_adjustment_at,
            ''),'|',ifnull(membership.future_cancel_fl,
            ''),'|',ifnull(membership.future_cancel_dt,
            ''),'|',ifnull(membership.oot_fl,
            ''),'|',ifnull(membership.address_change_dt,
            ''),'|',ifnull(membership.adm_d2000_update_dt,
            ''),'|',ifnull(membership.tier_ky,
            ''),'|',ifnull(membership.temp_addr_ind,
            ''),'|',ifnull(membership.previous_membership_id,
            ''),'|',ifnull(membership.previous_club_cd,
            ''),'|',ifnull(membership.transfer_club_cd,
            ''),'|',ifnull(membership.transfer_dt,
            ''),'|',ifnull(membership.dont_solicit_fl,
            ''),'|',ifnull(membership.bad_address_fl,
            ''),'|',ifnull(membership.dont_send_pub_fl,
            ''),'|',ifnull(membership.market_tracking_cd,
            ''),'|',ifnull(membership.salvage_fl,
            ''),'|',ifnull(membership.coverage_level_cd,
            ''),'|',ifnull(membership.group_cd,
           -- ''),'|',ifnull(membership.coverage_level_ky,
            ''),'|',ifnull(membership.ebill_fl,
            ''),'|',ifnull(membership.segmentation_cd,
            ''),'|',ifnull(membership.promo_code,
            ''),'|',ifnull(membership.phone_type,
            ''),'|',ifnull(phone.phone_adw_key,
          '-1'),'|',ifnull(product.product_adw_key,
          '-1'),'|',ifnull(aca_office.aca_office_adw_key,
          '-1'),'|',ifnull(CAST(membership_code.ERS_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.DSP_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SAL_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SHMC_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.STDNT_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NSAL_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NCOA_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.RET_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NMSP_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.DNTM_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.NINETY_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.AAAWON_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SHADOW_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.DNDM_CODE AS string),
            ''),'|',ifnull(CAST(membership_code.SIX_CODE AS string),
            '') ))) AS adw_row_hash
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_source` AS membership
  LEFT JOIN (
    SELECT
      membership_ky,
      MAX(CASE
          WHEN code='ERS' THEN 1
        ELSE
        0
      END
        ) AS ERS_CODE,
      MAX(CASE
          WHEN code='DSP' THEN 1
        ELSE
        0
      END
        ) AS DSP_CODE,
      MAX(CASE
          WHEN code='SAL' THEN 1
        ELSE
        0
      END
        ) AS SAL_CODE,
      MAX(CASE
          WHEN code='SHMC' THEN 1
        ELSE
        0
      END
        ) AS SHMC_CODE,
      MAX(CASE
          WHEN code='STDNT' THEN 1
        ELSE
        0
      END
        ) AS STDNT_CODE,
      MAX(CASE
          WHEN code='NSAL' THEN 1
        ELSE
        0
      END
        ) AS NSAL_CODE,
      MAX(CASE
          WHEN code='NCOA' THEN 1
        ELSE
        0
      END
        ) AS NCOA_CODE,
      MAX(CASE
          WHEN code='RET' THEN 1
        ELSE
        0
      END
        ) AS RET_CODE,
      MAX(CASE
          WHEN code='NMSP' THEN 1
        ELSE
        0
      END
        ) AS NMSP_CODE,
      MAX(CASE
          WHEN code='DNTM' THEN 1
        ELSE
        0
      END
        ) AS DNTM_CODE,
      MAX(CASE
          WHEN code='90' THEN 1
        ELSE
        0
      END
        ) AS NINETY_CODE,
      MAX(CASE
          WHEN code='AAAWON' THEN 1
        ELSE
        0
      END
        ) AS AAAWON_CODE,
      MAX(CASE
          WHEN code='SHADOW' THEN 1
        ELSE
        0
      END
        ) AS SHADOW_CODE,
      MAX(CASE
          WHEN code='DNDM' THEN 1
        ELSE
        0
      END
        ) AS DNDM_CODE,
      MAX(CASE
          WHEN code='6' THEN 1
        ELSE
        0
      END
        ) AS SIX_CODE
    FROM
      `{iu.INGESTION_PROJECT}.mzp.membership_code`
    GROUP BY
      1) AS membership_code
  ON
    membership.membership_ky = membership_code.membership_ky
    -- Take from Integration Layer
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone` AS phone
  ON
    membership.phone = phone.raw_phone_number
  LEFT JOIN 
    `{int_u.INTEGRATION_PROJECT}.adw.dim_product` AS product
  ON
    membership.coverage_level_cd = product.product_sku and product.active_indicator = 'Y'
  LEFT JOIN
    `{iu.INGESTION_PROJECT}.mzp.branch` as branch
  ON membership.branch_ky = branch.branch_ky
    left join `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` as aca_office
    	on branch.branch_cd = aca_office.membership_branch_code
    left join (select 
		        membership_id,
                previous_club_cd,
                previous_membership_id,
                previous_membership_ky,              
                ROW_NUMBER() OVER(PARTITION BY membership_id ORDER BY last_upd_dt DESC) AS dupe_check
               from `{iu.INGESTION_PROJECT}.mzp.membership_crossref`
              ) membership_crossref
     	        on membership.membership_id=membership_crossref.membership_id and membership_crossref.dupe_check = 1
  WHERE
    membership.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_stage` AS
  SELECT
    COALESCE(target.membership_adw_key,
      GENERATE_UUID()) membership_adw_key,
    source.address_adw_key,
    CAST (source.phone_adw_key AS string) phone_adw_key,
    CAST (source.product_adw_key AS string) product_adw_key,	
    CAST (source.aca_office_adw_key AS string) aca_office_adw_key,
    CAST (source.membership_source_system_key AS int64) membership_source_system_key,
    source.membership_identifier,
    source.membership_status_code,
    source.membership_billing_code,
    CAST(source.membership_status_datetime AS datetime) membership_status_datetime,
    CAST(substr (source.membership_cancel_date,
        0,
        10) AS date) membership_cancel_date,
    source.membership_billing_category_code,
    CAST(source.membership_dues_cost_amount AS numeric) membership_dues_cost_amount,
    CAST(source.membership_dues_adjustment_amount AS numeric) membership_dues_adjustment_amount,
    source.membership_future_cancel_indicator,
    CAST(substr (source.membership_future_cancel_date,
        0,
        10) AS date) membership_future_cancel_date,
    source.membership_out_of_territory_code,
    CAST(source.membership_address_change_datetime AS datetime) membership_address_change_datetime,
    CAST(source.membership_cdx_export_update_datetime AS datetime) membership_cdx_export_update_datetime,
    CAST(source.membership_tier_key AS float64) membership_tier_key,
    source.membership_temporary_address_indicator,
    source.previous_club_membership_identifier,
    source.previous_club_code,
    source.membership_transfer_club_code,
    CAST(source.membership_transfer_datetime AS datetime) membership_transfer_datetime,
    CAST(source.merged_membership_previous_club_code AS string) merged_membership_previous_club_code,
    CAST(source.merged_member_previous_membership_identifier AS string) merged_member_previous_membership_identifier,
    safe_cast(source.merged_member_previous_membership_key as int64) as merged_member_previous_membership_key,
    source.membership_dn_solicit_indicator,
    source.membership_bad_address_indicator,
    source.membership_dn_send_publications_indicator,
    source.membership_market_tracking_code,
    source.membership_salvage_flag,
    CAST(source.membership_salvaged_indicator AS string) membership_salvaged_indicator,
    CAST(source.membership_dn_salvage_indicator AS string) membership_dn_salvage_indicator,
    --source.membership_coverage_level_code,
    --CAST(source.membership_coverage_level_key AS int64) membership_coverage_level_key,
    source.membership_group_code,
    source.membership_type_code,
    source.membership_ebill_indicator,
    source.membership_marketing_segmentation_code,
    source.membership_promo_code,
    source.membership_phone_type_code,
    CAST(source.membership_ers_abuser_indicator AS string) membership_ers_abuser_indicator,
    CAST(source.membership_dn_send_aaaworld_indicator AS string) membership_dn_send_aaaworld_indicator,
    CAST(source.membership_shadow_motorcycle_indicator AS string) membership_shadow_motorcycle_indicator,
    CAST(source.membership_student_indicator AS string) membership_student_indicator,
    CAST(source.membership_address_updated_by_ncoa_indicator AS string) membership_address_updated_by_ncoa_indicator,
    CAST(source.membership_assigned_retention_indicator AS string) membership_assigned_retention_indicator,
    CAST(source.membership_no_membership_promotion_mailings_indicator AS string) membership_no_membership_promotion_mailings_indicator,
    CAST(source.membership_dn_telemarket_indicator AS string) membership_dn_telemarket_indicator,
    CAST(source.membership_dn_offer_plus_indicator AS string) membership_dn_offer_plus_indicator,
    CAST(source.membership_receive_aaaworld_online_edition_indicator AS string) membership_receive_aaaworld_online_edition_indicator,
    CAST(source.membership_shadowrv_coverage_indicator AS string) membership_shadowrv_coverage_indicator,
    CAST(source.membership_dn_direct_mail_solicit_indicator AS string) membership_dn_direct_mail_solicit_indicator,
    CAST(source.membership_heavy_ers_indicator AS string) membership_heavy_ers_indicator,
    CAST(source.membership_purge_indicator AS string) membership_purge_indicator,
    last_upd_dt AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership` target
  ON
    (source.membership_identifier=target.membership_identifier
      AND target.active_indicator='Y')
  WHERE
    target.membership_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
  UNION ALL
  SELECT
    target.membership_adw_key,
    target.address_adw_key,
    target.phone_adw_key,
    target.product_adw_key,	
    target.aca_office_adw_key,
    target.membership_source_system_key,
    target.membership_identifier,
    target.membership_status_code,
    target.membership_billing_code,
    target.membership_status_datetime,
    target.membership_cancel_date,
    source.membership_billing_category_code,
    target.membership_dues_cost_amount,
    target.membership_dues_adjustment_amount,
    target.membership_future_cancel_indicator,
    target.membership_future_cancel_date,
    target.membership_out_of_territory_code,
    target.membership_address_change_datetime,
    target.membership_cdx_export_update_datetime,
    target.membership_tier_key,
    target.membership_temporary_address_indicator,
    target.previous_club_membership_identifier,
    target.previous_club_code,
    target.membership_transfer_club_code,
    target.membership_transfer_datetime,
    target.merged_membership_previous_club_code,
    target.merged_member_previous_membership_identifier,
    target.merged_member_previous_membership_key,
    target.membership_dn_solicit_indicator,
    target.membership_bad_address_indicator,
    target.membership_dn_send_publications_indicator,
    target.membership_market_tracking_code,
    target.membership_salvage_flag,
    target.membership_salvaged_indicator,
    target.membership_dn_salvage_indicator,
    --target.membership_coverage_level_code,
    --target.membership_coverage_level_key,
    target.membership_group_code,
    target.membership_type_code,
    target.membership_ebill_indicator,
    target.membership_marketing_segmentation_code,
    target.membership_promo_code,
    target.membership_phone_type_code,
    target.membership_ers_abuser_indicator,
    target.membership_dn_send_aaaworld_indicator,
    target.membership_shadow_motorcycle_indicator,
    target.membership_student_indicator,
    target.membership_address_updated_by_ncoa_indicator,
    target.membership_assigned_retention_indicator,
    target.membership_no_membership_promotion_mailings_indicator,
    target.membership_dn_telemarket_indicator,
    target.membership_dn_offer_plus_indicator,
    target.membership_receive_aaaworld_online_edition_indicator,
    target.membership_shadowrv_coverage_indicator,
    target.membership_dn_direct_mail_solicit_indicator,
    target.membership_heavy_ers_indicator,
    target.membership_purge_indicator,
    target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 day) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership` target
  ON
    (source.membership_identifier=target.membership_identifier
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership
membership_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_work_stage` b
  ON
    (a.membership_adw_key = b.membership_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( 
    membership_adw_key, 
    address_adw_key, 
    phone_adw_key, 
    product_adw_key, 	
    aca_office_adw_key, 
    membership_source_system_key, 
    membership_identifier, 
    membership_status_code, 
    membership_billing_code, 
    membership_status_datetime, 
    membership_cancel_date, 
    membership_billing_category_code, 
    membership_dues_cost_amount, 
    membership_dues_adjustment_amount, 
    membership_future_cancel_indicator, 
    membership_future_cancel_date, 
    membership_out_of_territory_code, 
    membership_address_change_datetime, 
    membership_cdx_export_update_datetime, 
    membership_tier_key, 
    membership_temporary_address_indicator, 
    previous_club_membership_identifier, 
    previous_club_code, 
    membership_transfer_club_code, 
    membership_transfer_datetime, 
    merged_membership_previous_club_code, 
    merged_member_previous_membership_identifier, 
    merged_member_previous_membership_key, 
    membership_dn_solicit_indicator, 
    membership_bad_address_indicator, 
    membership_dn_send_publications_indicator, 
    membership_market_tracking_code, 
    membership_salvage_flag, 
    membership_salvaged_indicator, 
    membership_dn_salvage_indicator, 
  --  membership_coverage_level_code, 
  --  membership_coverage_level_key, 
    membership_group_code, 
    membership_type_code, 
    membership_ebill_indicator, 
    membership_marketing_segmentation_code, 
    membership_promo_code, 
    membership_phone_type_code, 
    membership_ers_abuser_indicator, 
    membership_dn_send_aaaworld_indicator, 
    membership_shadow_motorcycle_indicator, 
    membership_student_indicator, 
    membership_address_updated_by_ncoa_indicator, 
    membership_assigned_retention_indicator, 
    membership_no_membership_promotion_mailings_indicator, 
    membership_dn_telemarket_indicator, 
    membership_dn_offer_plus_indicator, 
    membership_receive_aaaworld_online_edition_indicator, 
    membership_shadowrv_coverage_indicator, 
    membership_dn_direct_mail_solicit_indicator, 
    membership_heavy_ers_indicator, 
    membership_purge_indicator, 
    effective_start_datetime, 
    effective_end_datetime, 
    active_indicator, 
    adw_row_hash, 
    integrate_insert_datetime, 
    integrate_insert_batch_number, 
    integrate_update_datetime, 
    integrate_update_batch_number ) VALUES (
    b.membership_adw_key,
    b.address_adw_key,
    b.phone_adw_key,
    b.product_adw_key,	
    b.aca_office_adw_key,
    b.membership_source_system_key,
    b.membership_identifier,
    b.membership_status_code,
    b.membership_billing_code,
    b.membership_status_datetime,
    b.membership_cancel_date,
    b.membership_billing_category_code,
    b.membership_dues_cost_amount,
    b.membership_dues_adjustment_amount,
    b.membership_future_cancel_indicator,
    b.membership_future_cancel_date,
    b.membership_out_of_territory_code,
    b.membership_address_change_datetime,
    b.membership_cdx_export_update_datetime,
    b.membership_tier_key,
    b.membership_temporary_address_indicator,
    b.previous_club_membership_identifier,
    b.previous_club_code,
    b.membership_transfer_club_code,
    b.membership_transfer_datetime,
    b.merged_membership_previous_club_code,
    b.merged_member_previous_membership_identifier,
    b.merged_member_previous_membership_key,
    b.membership_dn_solicit_indicator,
    b.membership_bad_address_indicator,
    b.membership_dn_send_publications_indicator,
    b.membership_market_tracking_code,
    b.membership_salvage_flag,
    b.membership_salvaged_indicator,
    b.membership_dn_salvage_indicator,
  --  b.membership_coverage_level_code,
  --  b.membership_coverage_level_key,
    b.membership_group_code,
    b.membership_type_code,
    b.membership_ebill_indicator,
    b.membership_marketing_segmentation_code,
    b.membership_promo_code,
    b.membership_phone_type_code,
    b.membership_ers_abuser_indicator,
    b.membership_dn_send_aaaworld_indicator,
    b.membership_shadow_motorcycle_indicator,
    b.membership_student_indicator,
    b.membership_address_updated_by_ncoa_indicator,
    b.membership_assigned_retention_indicator,
    b.membership_no_membership_promotion_mailings_indicator,
    b.membership_dn_telemarket_indicator,
    b.membership_dn_offer_plus_indicator,
    b.membership_receive_aaaworld_online_edition_indicator,
    b.membership_shadowrv_coverage_indicator,
    b.membership_dn_direct_mail_solicit_indicator,
    b.membership_heavy_ers_indicator,
    b.membership_purge_indicator,
    b.effective_start_datetime,
    b.effective_end_datetime,
    b.active_indicator,
    b.adw_row_hash,
    b.integrate_insert_datetime,
    b.integrate_insert_batch_number,
    b.integrate_update_datetime,
    b.integrate_update_batch_number 
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
"""
####################################################################################################################################
# Member
####################################################################################################################################
######################################################################
# Load Membership Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

member_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_source` AS
 SELECT
 member_source.MEMBER_KY ,
member_source.STATUS ,
member_source.CHECK_DIGIT_NR ,
member_source.ASSOCIATE_ID ,
member_source.MEMBERSHIP_ID ,
member_source.MEMBER_TYPE_CD ,
member_source.RENEW_METHOD_CD ,
member_source.COMMISSION_CD ,
member_source.SOURCE_OF_SALE ,
member_source.SOLICITATION_CD ,
member_source.ASSOCIATE_RELATION_CD,
member_source.JOIN_CLUB_DT,
member_source.JOIN_AAA_DT,
member_source.CANCEL_DT,
member_source.STATUS_DT,
member_source.BILLING_CATEGORY_CD,
member_source.BILLING_CD,
'N' as MEMBERSHIP_PURGE_INDICATOR,
member_source.MEMBER_EXPIRATION_DT,
member_source.MEMBER_CARD_EXPIRATION_DT,
member_source.REASON_JOINED,
member_source.DO_NOT_RENEW_FL,
member_source.FUTURE_CANCEL_DT,
member_source.FUTURE_CANCEL_FL,
member_source.FREE_MEMBER_FL,
member_source.PREVIOUS_MEMBERSHIP_ID,
member_source.PREVIOUS_CLUB_CD,
member_source.NAME_CHANGE_DT,
member_source.ADM_EMAIL_CHANGED_DT,
member_source.BAD_EMAIL_FL,
member_source.EMAIL_OPTOUT_FL,
member_source.WEB_LAST_LOGIN_DT,
member_source.ACTIVE_EXPIRATION_DT,
member_source.ACTIVATION_DT,
member_source.AGENT_ID,
member_source.LAST_UPD_DT,
ROW_NUMBER() OVER(PARTITION BY member_source.MEMBER_KY ORDER BY member_source.LAST_UPD_DT DESC) AS DUPE_CHECK
  FROM
  `{iu.INGESTION_PROJECT}.mzp.member` AS member_source
WHERE
  CAST(member_source.LAST_UPD_DT AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_member`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

member_work_transformed = f"""
  --Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
  -- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_transformed` AS
  SELECT
    adw_memship.membership_adw_key AS membership_adw_key,
    cust_src_key.contact_adw_key AS contact_adw_key,
    solicitation.membership_solicitation_adw_key,
    role.employee_role_adw_key AS employee_role_adw_key,
    member.member_ky AS member_source_system_key,
    member.membership_id AS membership_identifier,
    member.associate_id AS member_associate_identifier,
    member.check_digit_nr AS member_check_digit_number,
    member.status AS member_status_code,
    member.member_expiration_dt AS member_expiration_date,
    member.member_type_cd AS primary_member_indicator,
    member.do_not_renew_fl AS member_do_not_renew_indicator,
    member.billing_cd AS member_billing_code,
    member.billing_category_cd AS member_billing_category_code,
    member.member_card_expiration_dt AS member_card_expiration_dt,
    member.status_dt AS member_status_datetime,
    member.cancel_dt AS member_cancel_date,
    member.join_aaa_dt AS member_join_aaa_date,
    member.join_club_dt AS member_join_club_date,
    member.reason_joined AS member_reason_joined_code,
    member.associate_relation_cd AS member_associate_relation_code,
    member.solicitation_cd AS member_solicitation_code,
    member.source_of_sale AS member_source_of_sale_code,
    member.commission_cd AS commission_cd,
    member.future_cancel_fl AS member_future_cancel_indicator,
    member.future_cancel_dt AS member_future_cancel_date,
    member.renew_method_cd AS member_renew_method_code,
    member.free_member_fl AS free_member_indicator,
    member.previous_membership_id AS previous_club_membership_identifier,
    member.previous_club_cd AS member_previous_club_code,
    crossref.previous_member_ky AS merged_member_previous_key,
    crossref.previous_club_cd AS merged_member_previous_club_code,
    crossref.previous_membership_id AS merged_member_previous_membership_identifier,
    crossref.previous_associate_id AS merged_member_previous_associate_identifier,
    crossref.previous_check_digit_nr AS merged_member_previous_check_digit_number,
    crossref.previous_member16_id AS merged_member_previous_member16_identifier,
    member.name_change_dt AS member_name_change_datetime,
    member.adm_email_changed_dt AS member_email_changed_datetime,
    member.bad_email_fl AS member_bad_email_indicator,
    member.email_optout_fl AS member_email_optout_indicator,
    member.web_last_login_dt AS member_web_last_login_datetime,
    member.active_expiration_dt AS member_active_expiration_date,
    member.activation_dt AS member_activation_date,
    member_code.DNT_CODE AS member_dn_text_indicator,
    member_code.DEM_CODE AS member_duplicate_email_indicator,
    member_code.DNC_CODE AS member_dn_call_indicator,
    member_code.NOE_CODE AS member_no_email_indicator,
    member_code.DNM_CODE AS member_dn_mail_indicator,
    member_code.DNAE_CODE AS member_dn_ask_for_email_indicator,
    member_code.DNE_CODE AS member_dn_email_indicator,
    member_code.RFE_CODE AS member_refused_give_email_indicator,
    member.membership_purge_indicator AS membership_purge_indicator,
    CAST (member.last_upd_dt AS datetime) AS last_upd_dt,
    TO_BASE64(MD5(CONCAT(ifnull(member.membership_id,
            ''),'|', ifnull(member.associate_id,
            ''),'|', ifnull(member.check_digit_nr,
            ''),'|', ifnull(member.status,
            ''),'|', ifnull(member.member_expiration_dt,
            ''),'|', ifnull(member.member_type_cd,
            ''),'|', ifnull(member.do_not_renew_fl,
            ''),'|', ifnull(member.billing_cd,
            ''),'|', ifnull(member.billing_category_cd,
            ''),'|', ifnull(member.member_card_expiration_dt,
            ''),'|', ifnull(member.status_dt,
            ''),'|', ifnull(member.cancel_dt,
            ''),'|', ifnull(member.join_aaa_dt,
            ''),'|', ifnull(member.join_club_dt,
            ''),'|', ifnull(member.reason_joined,
            ''),'|', ifnull(member.associate_relation_cd,
            ''),'|', ifnull(member.solicitation_cd,
            ''),'|', ifnull(member.source_of_sale,
            ''),'|', ifnull(member.commission_cd,
            ''),'|', ifnull(member.future_cancel_fl,
            ''),'|', ifnull(member.future_cancel_dt,
            ''),'|', ifnull(member.renew_method_cd,
            ''),'|', ifnull(member.free_member_fl,
            ''),'|', ifnull(member.previous_membership_id,
            ''),'|', ifnull(member.previous_club_cd,
            ''),'|', ifnull(crossref.previous_member_ky,
            ''),'|', ifnull(crossref.previous_club_cd,
            ''),'|', ifnull(crossref.previous_membership_id,
            ''),'|', ifnull(crossref.previous_associate_id,
            ''),'|', ifnull(crossref.previous_check_digit_nr,
            ''),'|', ifnull(crossref.previous_member16_id,
            ''),'|', ifnull(member.name_change_dt,
            ''),'|', ifnull(member.adm_email_changed_dt,
            ''),'|', ifnull(member.bad_email_fl,
            ''),'|', ifnull(member.email_optout_fl,
            ''),'|', ifnull(member.web_last_login_dt,
            ''),'|', ifnull(member.active_expiration_dt,
            ''),'|', ifnull(member.activation_dt,
            ''),'|', ifnull(adw_memship.membership_adw_key,
            '-1'),'|', ifnull(cust_src_key.contact_adw_key,
            '-1'),'|', ifnull(solicitation.membership_solicitation_adw_key,
            '-1'),'|', ifnull(role.employee_role_adw_key,
            '-1'),'|', ifnull(SAFE_CAST(member_code.DNT_CODE AS STRING),
            ''),'|', ifnull(SAFE_CAST(member_code.DEM_CODE AS STRING),
            ''),'|', ifnull(SAFE_CAST(member_code.DNC_CODE AS STRING),
            ''),'|', ifnull(SAFE_CAST(member_code.NOE_CODE AS STRING),
            ''),'|', ifnull(SAFE_CAST(member_code.DNM_CODE AS STRING),
            ''),'|', ifnull(SAFE_CAST(member_code.DNAE_CODE AS STRING),
            ''),'|', ifnull(SAFE_CAST(member_code.DNE_CODE AS STRING),
            ''),'|', ifnull(SAFE_CAST(member_code.RFE_CODE AS STRING),
            ''),'|', ifnull(member.membership_purge_indicator,
            ''),'|' ))) AS adw_row_hash
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_source` AS member
  LEFT JOIN (
    SELECT
      member_ky,
      MAX(CASE
          WHEN code='DNT' THEN 1
        ELSE
        0
      END
        ) AS DNT_CODE,
      MAX(CASE
          WHEN code='DEM' THEN 1
        ELSE
        0
      END
        ) AS DEM_CODE,
      MAX(CASE
          WHEN code='DNC' THEN 1
        ELSE
        0
      END
        ) AS DNC_CODE,
      MAX(CASE
          WHEN code='NOE' THEN 1
        ELSE
        0
      END
        ) AS NOE_CODE,
      MAX(CASE
          WHEN code='DNM' THEN 1
        ELSE
        0
      END
        ) AS DNM_CODE,
      MAX(CASE
          WHEN code='DNAE' THEN 1
        ELSE
        0
      END
        ) AS DNAE_CODE,
      MAX(CASE
          WHEN code='DNE' THEN 1
        ELSE
        0
      END
        ) AS DNE_CODE,
      MAX(CASE
          WHEN code='RFE' THEN 1
        ELSE
        0
      END
        ) AS RFE_CODE
    FROM
      `{iu.INGESTION_PROJECT}.mzp.member_code`
    GROUP BY
      1) AS member_code
  ON
    member.member_ky = member_code.member_ky
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership` adw_memship
  ON
    member.member_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING)
    AND active_indicator='Y'
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key` cust_src_key
  ON
    member.member_ky=cust_src_key.source_1_key
    AND cust_src_key.key_type_name LIKE 'member_key%'
    AND cust_src_key.active_indicator='Y'
  LEFT JOIN (
    SELECT
      member_ky,
      previous_member_ky,
      previous_club_cd,
      previous_membership_id,
      previous_associate_id,
      previous_check_digit_nr,
      previous_member16_id,
      ROW_NUMBER() OVER(PARTITION BY member_ky ORDER BY NULL DESC) AS DUPE_CHECK
    FROM
      `{iu.INGESTION_PROJECT}.mzp.member_crossref`) crossref
  ON
    crossref.member_ky=member.member_ky
    AND crossref.DUPE_CHECK=1
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role` role
  ON
    member.agent_id=role.employee_role_id
    AND role.active_indicator='Y'
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation` solicitation
  ON
    solicitation.solicitation_code=member.solicitation_cd
    AND solicitation.active_indicator='Y'
  WHERE
    member.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

member_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_stage` AS
SELECT
COALESCE(target.member_adw_key,GENERATE_UUID()) AS member_adw_key,
source.membership_adw_key membership_adw_key,
source.contact_adw_key contact_adw_key,
source.membership_solicitation_adw_key,
source.employee_role_adw_key employee_role_adw_key,
SAFE_CAST(source.member_source_system_key AS INT64) member_source_system_key,
source.membership_identifier membership_identifier,
source.member_associate_identifier member_associate_identifier,
SAFE_CAST(source.member_check_digit_number AS INT64) member_check_digit_number,
source.member_status_code member_status_code,
CAST(substr(source.member_expiration_date,0,10) AS date) as member_expiration_date,
CAST(substr(source.member_card_expiration_dt,0,10) AS date) as  member_card_expiration_dt,
source.primary_member_indicator primary_member_indicator,
source.member_do_not_renew_indicator member_do_not_renew_indicator,
source.member_billing_code member_billing_code,
source.member_billing_category_code member_billing_category_code,
CAST(source.member_status_datetime as datetime) as member_status_datetime,
CAST(substr(source.member_cancel_date,0,10) AS date) as member_cancel_date,
CASE WHEN SUBSTR(source.member_join_aaa_date,1,3) IN ('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
    THEN parse_date('%h %d %Y',substr(source.member_join_aaa_date,0,11))
    ELSE parse_date('%d-%b-%y',substr(source.member_join_aaa_date,0,10))
    END as member_join_aaa_date,
CAST(substr(source.member_join_club_date,0,10) AS date) as member_join_club_date,
source.member_reason_joined_code member_reason_joined_code,
source.member_associate_relation_code member_associate_relation_code,
source.member_solicitation_code member_solicitation_code,
source.member_source_of_sale_code member_source_of_sale_code,
source.commission_cd commission_cd,
source.member_future_cancel_indicator member_future_cancel_indicator,
CAST(substr(source.member_future_cancel_date,0,10) AS date) as member_future_cancel_date,
source.member_renew_method_code member_renew_method_code,
source.free_member_indicator free_member_indicator,
source.previous_club_membership_identifier previous_club_membership_identifier,
source.member_previous_club_code member_previous_club_code,
safe_cast(source.merged_member_previous_key as int64) merged_member_previous_key,
source.merged_member_previous_club_code merged_member_previous_club_code,
source.merged_member_previous_membership_identifier merged_member_previous_membership_identifier,
source.merged_member_previous_associate_identifier merged_member_previous_associate_identifier,
source.merged_member_previous_check_digit_number merged_member_previous_check_digit_number,
source.merged_member_previous_member16_identifier merged_member_previous_member16_identifier,
SAFE_CAST(source.member_name_change_datetime AS DATETIME) AS member_name_change_datetime,
SAFE_CAST(source.member_email_changed_datetime AS DATETIME) member_email_changed_datetime,
source.member_bad_email_indicator member_bad_email_indicator,
source.member_email_optout_indicator member_email_optout_indicator,
SAFE_CAST(source.member_web_last_login_datetime AS DATETIME) member_web_last_login_datetime,
CAST(substr (source.member_active_expiration_date,0,10) AS date) AS member_active_expiration_date,
CAST(substr (source.member_activation_date,0,10) AS date) AS member_activation_date,
SAFE_CAST(source.member_dn_text_indicator AS STRING) member_dn_text_indicator,
SAFE_CAST(source.member_duplicate_email_indicator AS STRING) member_duplicate_email_indicator,
SAFE_CAST(source.member_dn_call_indicator AS STRING) member_dn_call_indicator,
SAFE_CAST(source.member_no_email_indicator AS STRING) member_no_email_indicator,
SAFE_CAST(source.member_dn_mail_indicator AS STRING) member_dn_mail_indicator,
SAFE_CAST(source.member_dn_ask_for_email_indicator AS STRING) member_dn_ask_for_email_indicator,
SAFE_CAST(source.member_dn_email_indicator AS STRING) member_dn_email_indicator,
SAFE_CAST(source.member_refused_give_email_indicator AS STRING) member_refused_give_email_indicator,
SAFE_CAST(source.membership_purge_indicator AS STRING) membership_purge_indicator,
source.last_upd_dt AS effective_start_datetime,
CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number

 FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` target
  ON
    (source.member_source_system_key =SAFE_CAST(target.member_source_system_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.member_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
target.member_adw_key,
target.membership_adw_key,
target.contact_adw_key,
target.membership_solicitation_adw_key,
target.employee_role_adw_key,
target.member_source_system_key,
target.membership_identifier,
target.member_associate_identifier,
target.member_check_digit_number,
target.member_status_code,
target.member_expiration_date,
target.member_card_expiration_date ,
target.primary_member_indicator,
target.member_do_not_renew_indicator,
target.member_billing_code,
target.member_billing_category_code,
target.member_status_datetime,
target.member_cancel_date,
target.member_join_aaa_date,
target.member_join_club_date,
target.member_reason_joined_code,
target.member_associate_relation_code,
target.member_solicitation_code,
target.member_source_of_sale_code,
target.member_commission_code ,
target.member_future_cancel_indicator,
target.member_future_cancel_date,
target.member_renew_method_code,
target.free_member_indicator,
target.previous_club_membership_identifier,
target.previous_club_code,
target.merged_member_previous_key,
target.merged_member_previous_club_code,
target.merged_member_previous_membership_identifier,
target.merged_member_previous_associate_identifier,
target.merged_member_previous_check_digit_number,
target.merged_member_previous_member16_identifier,
target.member_name_change_datetime,
target.member_email_changed_datetime,
target.member_bad_email_indicator,
target.member_email_optout_indicator,
target.member_web_last_login_datetime,
target.member_active_expiration_date,
target.member_activation_date,
target.member_dn_text_indicator,
target.member_duplicate_email_indicator,
target.member_dn_call_indicator,
target.member_no_email_indicator,
target.member_dn_mail_indicator,
target.member_dn_ask_for_email_indicator,
target.member_dn_email_indicator,
target.member_refused_give_email_indicator,
target.membership_purge_indicator,
target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` target
  ON
    (source.member_source_system_key =SAFE_CAST(target.member_source_system_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Member
member_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_work_stage` b
    ON (a.member_adw_key = b.member_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
    member_adw_key,
membership_adw_key,
contact_adw_key,
membership_solicitation_adw_key,
employee_role_adw_key,
member_source_system_key,
membership_identifier,
member_associate_identifier,
member_check_digit_number,
member_status_code,
member_expiration_date,
member_card_expiration_date,
primary_member_indicator,
member_do_not_renew_indicator,
member_billing_code,
member_billing_category_code,
member_status_datetime,
member_cancel_date,
member_join_aaa_date,
member_join_club_date,
member_reason_joined_code,
member_associate_relation_code,
member_solicitation_code,
member_source_of_sale_code,
member_commission_code,
member_future_cancel_indicator,
member_future_cancel_date,
member_renew_method_code,
free_member_indicator,
previous_club_membership_identifier,
previous_club_code,
merged_member_previous_key,
merged_member_previous_club_code,
merged_member_previous_membership_identifier,
merged_member_previous_associate_identifier,
merged_member_previous_check_digit_number,
merged_member_previous_member16_identifier,
member_name_change_datetime,
member_email_changed_datetime,
member_bad_email_indicator,
member_email_optout_indicator,
member_web_last_login_datetime,
member_active_expiration_date,
member_activation_date,
member_dn_text_indicator,
member_duplicate_email_indicator,
member_dn_call_indicator,
member_no_email_indicator,
member_dn_mail_indicator,
member_dn_ask_for_email_indicator,
member_dn_email_indicator,
member_refused_give_email_indicator,
membership_purge_indicator,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
    ) VALUES
    (
    b.member_adw_key,
b.membership_adw_key,
b.contact_adw_key,
b.membership_solicitation_adw_key,
b.employee_role_adw_key,
b.member_source_system_key,
b.membership_identifier,
b.member_associate_identifier,
b.member_check_digit_number,
b.member_status_code,
b.member_expiration_date,
b.member_card_expiration_dt,
b.primary_member_indicator,
b.member_do_not_renew_indicator,
b.member_billing_code,
b.member_billing_category_code,
b.member_status_datetime,
b.member_cancel_date,
b.member_join_aaa_date,
b.member_join_club_date,
b.member_reason_joined_code,
b.member_associate_relation_code,
b.member_solicitation_code,
b.member_source_of_sale_code,
b.commission_cd,
b.member_future_cancel_indicator,
b.member_future_cancel_date,
b.member_renew_method_code,
b.free_member_indicator,
b.previous_club_membership_identifier,
b.member_previous_club_code,
b.merged_member_previous_key,
b.merged_member_previous_club_code,
b.merged_member_previous_membership_identifier,
b.merged_member_previous_associate_identifier,
b.merged_member_previous_check_digit_number,
b.merged_member_previous_member16_identifier,
b.member_name_change_datetime,
b.member_email_changed_datetime,
b.member_bad_email_indicator,
b.member_email_optout_indicator,
b.member_web_last_login_datetime,
b.member_active_expiration_date,
b.member_activation_date,
b.member_dn_text_indicator,
b.member_duplicate_email_indicator,
b.member_dn_call_indicator,
b.member_no_email_indicator,
b.member_dn_mail_indicator,
b.member_dn_ask_for_email_indicator,
b.member_dn_email_indicator,
b.member_refused_give_email_indicator,
b.membership_purge_indicator,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number
    )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number

"""

####################################################################################################################################
# Member_rider
####################################################################################################################################

#################################
# Member_rider - Stage Load
#################################
# Member_rider

######################################################################
# Load Member_rider Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


member_rider_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_source` AS
SELECT
membership_ky,
agent_id,
rider_comp_cd,
AUTORENEWAL_CARD_KY,
rider.STATUS_DT  as member_rider_status_datetime
,rider.STATUS as member_rider_status_code
,rider_effective_dt  as member_rider_effective_date
,rider.CANCEL_DT  as member_rider_cancel_date
,CANCEL_REASON_CD as member_rider_cancel_reason_code
,rider.BILLING_CATEGORY_CD as member_rider_billing_category_code
,rider.SOLICITATION_CD  as member_rider_solicitation_code
,COST_EFFECTIVE_DT  as member_rider_cost_effective_date
,ACTUAL_CANCEL_DT  as member_rider_actual_cancel_datetime
,RIDER_KY  as member_rider_source_key
,rider.DO_NOT_RENEW_FL as member_rider_do_not_renew_indicator
,DUES_COST_AT  as member_rider_dues_cost_amount
,DUES_ADJUSTMENT_AT  as member_rider_dues_adjustment_amount
,PAYMENT_AT  as member_rider_payment_amount
,PAID_BY_CD as member_rider_paid_by_code
,rider.FUTURE_CANCEL_DT  as member_rider_future_cancel_date
,REINSTATE_FL as member_rider_reinstate_indicator
,ADM_ORIGINAL_COST_AT  as member_rider_adm_original_cost_amount
,REINSTATE_REASON_CD as member_rider_reinstate_reason_code
,EXTEND_EXPIRATION_AT  as member_rider_extend_expiration_amount
,RIDER_DEFINITION_KY  as member_rider_definition_key
,ACTIVATION_DT  as member_rider_activation_date
,rider.last_upd_dt  ,
    ROW_NUMBER() OVER (PARTITION BY RIDER_KY ORDER BY last_upd_dt DESC ) AS DUP_CHECK
  FROM
    `{iu.INGESTION_PROJECT}.mzp.rider` rider
    WHERE
  CAST(rider.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################
member_rider_work_transformed = f"""
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_transformed` AS
SELECT
coalesce(member_adw_key,'-1') as member_adw_key,
coalesce(membership.membership_adw_key,'-1') as membership_adw_key,
coalesce(product.product_adw_key,'-1') as product_adw_key,
coalesce(employee_role.employee_role_adw_key,'-1') as employee_role_adw_key,
member_rider_status_datetime
,member_rider_status_code
,member_rider_effective_date
,member_rider_cancel_date
,member_rider_cancel_reason_code
,member_rider_billing_category_code
,member_rider_solicitation_code
,member_rider_cost_effective_date
,member_rider_actual_cancel_datetime
,coalesce(auto_renewal.member_auto_renewal_card_adw_key,'-1') as member_auto_renewal_card_adw_key
,member_rider_source_key
,member_rider_do_not_renew_indicator
,member_rider_dues_cost_amount
,member_rider_dues_adjustment_amount
,member_rider_payment_amount
,member_rider_paid_by_code
,member_rider_future_cancel_date
,member_rider_reinstate_indicator
,member_rider_adm_original_cost_amount
,member_rider_reinstate_reason_code
,member_rider_extend_expiration_amount
,member_rider_definition_key
,member_rider_activation_date
,CAST (rider.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(COALESCE(member_rider_status_datetime,'')
,COALESCE(member_rider_status_code,'')
,COALESCE(member_rider_effective_date,'')
,COALESCE(member_rider_cancel_date,'')
,COALESCE(member_rider_cancel_reason_code,'')
,COALESCE(member_rider_billing_category_code,'')
,COALESCE(member_rider_solicitation_code,'')
,COALESCE(member_rider_cost_effective_date,'')
,COALESCE(member_rider_actual_cancel_datetime,'')
,COALESCE(member_rider_source_key,'')
,COALESCE(member_rider_do_not_renew_indicator,'')
,COALESCE(member_rider_dues_cost_amount,'')
,COALESCE(member_rider_dues_adjustment_amount,'')
,COALESCE(member_rider_payment_amount,'')
,COALESCE(member_rider_paid_by_code,'')
,COALESCE(member_rider_future_cancel_date,'')
,COALESCE(member_rider_reinstate_indicator,'')
,COALESCE(member_rider_adm_original_cost_amount,'')
,COALESCE(member_rider_reinstate_reason_code,'')
,COALESCE(member_rider_extend_expiration_amount,'')
,COALESCE(member_rider_definition_key,'')
,COALESCE(member_rider_activation_date,'')
,COALESCE(member_adw_key,'-1')
,COALESCE(product.product_adw_key,'-1')
,COALESCE(auto_renewal.member_auto_renewal_card_adw_key,'-1')
,COALESCE(employee_role.employee_role_adw_key,'-1')
,COALESCE(membership.membership_adw_key,'-1')
))) as adw_row_hash
,
 ROW_NUMBER() OVER (PARTITION BY member_rider_source_key ORDER BY last_upd_dt DESC ) AS DUP_CHECK
FROM
 `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_source` AS rider
left  join
`{int_u.INTEGRATION_PROJECT}.member.dim_member` member
on
SAFE_CAST(rider.member_rider_source_key as INT64)=member.member_source_system_key and member.active_indicator='Y'
left  join `{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership on SAFE_CAST(rider.membership_ky as INT64) =membership.membership_source_system_key and membership.active_indicator='Y'
left  join `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role` employee_role on employee_role.employee_role_id=rider.agent_id and employee_role.active_indicator='Y'
left join `{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` auto_renewal on auto_renewal.member_source_arc_key=CAST(rider.AUTORENEWAL_CARD_KY as INT64)
left  join (select product.* from `{int_u.INTEGRATION_PROJECT}.adw.dim_product` product join `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` dim_product_category ON product_category_code IN  ('RCC','MBR_FEES') and dim_product_category.product_category_adw_key =product.product_category_adw_key  ) product
on rider.rider_comp_cd=product.product_sku and product.active_indicator='Y' where rider.dup_check=1


"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

member_rider_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_stage` AS
SELECT
    COALESCE(target.member_rider_adw_key,GENERATE_UUID()) member_rider_adw_key,
SAFE_CAST(source.membership_adw_key as STRING) membership_adw_key,
SAFE_CAST(source.member_adw_key as STRING) member_adw_key,
source.member_auto_renewal_card_adw_key ,
source.employee_role_adw_key,
source.product_adw_key,
SAFE_CAST(source.member_rider_source_key AS INT64) member_rider_source_key,
source.member_rider_status_code,
SAFE_CAST(source.member_rider_status_datetime as DATETIME) member_rider_status_datetime,
SAFE_CAST(source.member_rider_cancel_date as DATE) member_rider_cancel_date,
SAFE_CAST(source.member_rider_cost_effective_date as DATE) member_rider_cost_effective_date,
source.member_rider_solicitation_code,
source.member_rider_do_not_renew_indicator,
SAFE_CAST(source.member_rider_dues_cost_amount as NUMERIC) member_rider_dues_cost_amount,
SAFE_CAST(source.member_rider_dues_adjustment_amount as NUMERIC) member_rider_dues_adjustment_amount,
SAFE_CAST(source.member_rider_payment_amount as NUMERIC) member_rider_payment_amount ,
source.member_rider_paid_by_code,
SAFE_CAST(source.member_rider_future_cancel_date as DATE) member_rider_future_cancel_date,
source.member_rider_billing_category_code,
source.member_rider_cancel_reason_code,
source.member_rider_reinstate_indicator,
SAFE_CAST(source.member_rider_effective_date as DATE) member_rider_effective_date,
SAFE_CAST(source.member_rider_adm_original_cost_amount as NUMERIC) member_rider_adm_original_cost_amount,
source.member_rider_reinstate_reason_code,
SAFE_CAST(source.member_rider_extend_expiration_amount as NUMERIC) member_rider_extend_expiration_amount,
SAFE_CAST(source.member_rider_actual_cancel_datetime as DATETIME) member_rider_actual_cancel_datetime ,
SAFE_CAST(source.member_rider_definition_key as INT64) member_rider_definition_key,
SAFE_CAST(source.member_rider_activation_date as DATE) member_rider_activation_date,
last_upd_dt AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
from
 `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider` target
  ON
    (SAFE_CAST(source.member_rider_source_key as INT64)=target.member_rider_source_key
      AND target.active_indicator='Y')
  WHERE
    target.member_rider_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
  SELECT
   target.member_rider_adw_key	,
target.membership_adw_key	,
target.member_adw_key	,
target.member_auto_renewal_card_adw_key,
target.employee_role_adw_key	,
target.product_adw_key	,
target.member_rider_source_key	,
target.member_rider_status_code	,
target.member_rider_status_datetime	,
target.member_rider_cancel_date		,
target.member_rider_cost_effective_date	,
target.member_rider_solicitation_code	,
target.member_rider_do_not_renew_indicator	,
target.member_rider_dues_cost_amount,
target.member_rider_dues_adjustment_amount	,
target.member_rider_payment_amount,
target.member_rider_paid_by_code	,
target.member_rider_future_cancel_date	,
target.member_rider_billing_category_code	,
target.member_rider_cancel_reason_code	,
target.member_rider_reinstate_indicator	,
target.member_rider_effective_date		,
target.member_rider_adm_original_cost_amount,
target.member_rider_reinstate_reason_code	,
target.member_rider_extend_expiration_amount,
target.member_rider_actual_cancel_datetime	,
target.member_rider_definition_key			,
target.member_rider_activation_date		,
    target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 day) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider` target
  ON
    (SAFE_CAST(source.member_rider_source_key as  INT64)=target.member_rider_source_key
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Member_rider
member_rider_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_rider_work_stage` b
  ON
    (a.member_rider_adw_key = b.member_rider_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( member_rider_adw_key			        ,
membership_adw_key			            ,
member_adw_key			                ,
member_auto_renewal_card_adw_key			,
employee_role_adw_key			        ,
product_adw_key			                ,
member_rider_source_key		            ,
member_rider_status_code			    ,
member_rider_status_datetime		    ,
member_rider_cancel_date		        ,
member_rider_cost_effective_date		,
member_rider_solicitation_code			,
member_rider_do_not_renew_indicator		,
member_rider_dues_cost_amount			,
member_rider_dues_adjustment_amount		,
member_rider_payment_amount			    ,
member_rider_paid_by_code			    ,
member_rider_future_cancel_date		    ,
member_rider_billing_category_code		,
member_rider_cancel_reason_code			,
member_rider_reinstate_indicator		,
member_rider_effective_date		        ,
member_rider_adm_original_cost_amount	,
member_rider_reinstate_reason_code		,
member_rider_extend_expiration_amount	,
member_rider_actual_cancel_datetime		,
member_rider_definition_key		        ,
member_rider_activation_date		    ,
effective_start_datetime		        ,
effective_end_datetime		            ,
active_indicator			            ,
adw_row_hash			                ,
integrate_insert_datetime		        ,
integrate_insert_batch_number		    ,
integrate_update_datetime		        ,
integrate_update_batch_number	         ) VALUES (b.member_rider_adw_key			        ,
b.membership_adw_key			            ,
b.member_adw_key			                ,
b.member_auto_renewal_card_adw_key			,
b.employee_role_adw_key			        ,
b.product_adw_key			                ,
b.member_rider_source_key		            ,
b.member_rider_status_code			    ,
b.member_rider_status_datetime		    ,
b.member_rider_cancel_date		        ,
b.member_rider_cost_effective_date		,
b.member_rider_solicitation_code			,
b.member_rider_do_not_renew_indicator		,
b.member_rider_dues_cost_amount			,
b.member_rider_dues_adjustment_amount		,
b.member_rider_payment_amount			    ,
b.member_rider_paid_by_code			    ,
b.member_rider_future_cancel_date		    ,
b.member_rider_billing_category_code		,
b.member_rider_cancel_reason_code			,
b.member_rider_reinstate_indicator		,
b.member_rider_effective_date		        ,
b.member_rider_adm_original_cost_amount	,
b.member_rider_reinstate_reason_code		,
b.member_rider_extend_expiration_amount	,
b.member_rider_actual_cancel_datetime		,
b.member_rider_definition_key		        ,
b.member_rider_activation_date		    ,
b.effective_start_datetime		        ,
b.effective_end_datetime		            ,
b.active_indicator			            ,
b.adw_row_hash			                ,
b.integrate_insert_datetime		        ,
b.integrate_insert_batch_number		    ,
b.integrate_update_datetime		        ,
b.integrate_update_batch_number
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number


"""

####################################################################################################################################
# Membership Comments
####################################################################################################################################

membership_comment_source = f"""
CREATE OR REPLACE TABLE `{int_u.INTEGRATION_PROJECT}.adw_work.membership_comment_source`  AS
SELECT
  membership_comment_ky,
  membership_ky,
  create_dt,
  create_user_id,
  comment_type_cd,
  comments,
  assign_user_id,
  resolved_dt,
  resolved_user_id,
  resolution,
  last_upd_dt,
  assign_dept_cd,
  adw_lake_insert_datetime,
  adw_lake_insert_batch_number,
  ROW_NUMBER() OVER (PARTITION BY membership_comment_ky, membership_ky ORDER BY last_upd_dt DESC) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.membership_comment` source
"""

membership_comment_fkey = f"""
CREATE OR REPLACE TABLE `{int_u.INTEGRATION_PROJECT}.adw_work.membership_comment_fkey` AS
WITH employee_lookup AS (
  SELECT
    aa.user_id,
    bb.employee_adw_key
  FROM
    `adw-lake-dev.mzp.cx_iusers` aa
  LEFT JOIN (
    SELECT
      employee_adw_key,
      employee_active_directory_user_identifier AS username
    FROM
      `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee`
    WHERE
      active_indicator = 'Y' ) bb
  ON
    (aa.username = bb.username)
), membership_lookup AS (
    SELECT 
      membership_adw_key as comment_relation_adw_key,
      CAST(membership_source_system_key AS String) AS comment_source_system_key
    FROM 
      `{int_u.INTEGRATION_PROJECT}.member.dim_membership`
    WHERE 
      active_indicator = 'Y'
)
SELECT
  COALESCE(bb.employee_adw_key,'-1') AS created_employee_adw_key,
  COALESCE(cc.employee_adw_key,'-1') AS resolved_employee_adw_key,
  COALESCE(dd.comment_relation_adw_key,'-1') AS comment_relation_adw_key,
  'MZP' AS comment_source_system_name,
  CAST(comment_source_system_key AS INT64) AS comment_source_system_key,
  SAFE_CAST(create_dt AS DATETIME) AS comment_creation_datetime,
  comments AS comment_text,
  SAFE_CAST(resolved_dt AS DATETIME) AS comment_resolved_datetime,
  resolution AS comment_resolution_text,
  SAFE_CAST(last_upd_dt AS DATETIME)  AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT(
    COALESCE(create_user_id,''),
    COALESCE(resolved_user_id,''),
    COALESCE(membership_ky,''),
    COALESCE(comment_source_system_key,''),
    COALESCE(create_dt,''),
    COALESCE(comments,''),
    COALESCE(resolved_dt,''),
    COALESCE(resolution,'')
  ))) AS adw_row_hash
FROM 
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_comment_source` aa
LEFT JOIN 
   employee_lookup bb ON (aa.create_user_id = bb.user_id)
LEFT JOIN 
   employee_lookup cc ON (aa.resolved_user_id = cc.user_id)
LEFT JOIN 
   membership_lookup dd ON (aa.membership_ky = dd.comment_source_system_key)
WHERE dupe_check = 1
"""

membership_comment_stage = f"""
CREATE OR REPLACE TABLE `{int_u.INTEGRATION_PROJECT}.adw_work.membership_comment_stage` AS
SELECT
  COALESCE(b.comment_adw_key,GENERATE_UUID()) AS comment_adw_key,
  a.created_employee_adw_key,
  a.resolved_employee_adw_key,
  a.comment_relation_adw_key,
  a.comment_source_system_name,
  a.comment_source_system_key,
  a.comment_creation_datetime,
  a.comment_text,
  a.comment_resolved_datetime,
  a.comment_resolution_text,
  a.effective_start_datetime,
  CAST('9999-12-31' AS datetime) AS effective_end_datetime,
  'Y' AS active_indicator,
  current_datetime AS integrate_insert_datetime,
  1 AS integrate_insert_batch_number,
  current_datetime AS integrate_update_datetime,
  1 AS integrate_update_batch_number,
  a.adw_row_hash
FROM 
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_comment_fkey` a
LEFT JOIN 
  `{int_u.INTEGRATION_PROJECT}.adw.dim_comment` b ON ( 
   a.comment_source_system_key = b.comment_source_system_key
   AND b.active_indicator='Y')
WHERE 
  b.comment_relation_adw_key IS NULL 
  OR a.adw_row_hash <> b.adw_row_hash
UNION ALL
SELECT
  b.comment_adw_key AS comment_adw_key,
  b.created_employee_adw_key,
  b.resolved_employee_adw_key,
  b.comment_relation_adw_key,
  b.comment_source_system_name,
  b.comment_source_system_key,
  b.comment_creation_datetime,
  b.comment_text,
  b.comment_resolved_datetime,
  b.comment_resolution_text,
  b.effective_start_datetime,
  DATETIME_SUB(a.effective_start_datetime, INTERVAL 1 day) AS effective_end_datetime,
  'N' AS active_indicator,
  b.integrate_insert_datetime AS integrate_insert_datetime,
  b.integrate_insert_batch_number AS integrate_insert_batch_number,
  current_datetime AS integrate_update_datetime,
  1 AS integrate_update_batch_number,
  b.adw_row_hash
FROM 
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_comment_fkey` a
JOIN 
  `{int_u.INTEGRATION_PROJECT}.adw.dim_comment` b ON (
    a.comment_source_system_key = b.comment_source_system_key 
    AND b.active_indicator = 'Y')
WHERE a.adw_row_hash <> b.adw_row_hash
"""

membership_comment = f"""
MERGE INTO 
  `{int_u.INTEGRATION_PROJECT}.adw.dim_comment` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_comment_stage` b
  ON (a.comment_source_system_key = b.comment_source_system_key
  AND a.effective_start_datetime = b.effective_start_datetime)
WHEN NOT MATCHED THEN INSERT (
  comment_adw_key,
  created_employee_adw_key,
  resolved_employee_adw_key,
  comment_relation_adw_key,
  comment_source_system_name,
  comment_source_system_key,
  comment_creation_datetime,
  comment_text,
  comment_resolved_datetime,
  comment_resolution_text,
  effective_start_datetime,
  effective_end_datetime,
  active_indicator,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number,
  adw_row_hash
  )
VALUES (
  b.comment_adw_key,
  b.created_employee_adw_key,
  b.resolved_employee_adw_key,
  b.comment_relation_adw_key,
  b.comment_source_system_name,
  b.comment_source_system_key,
  b.comment_creation_datetime,
  b.comment_text,
  b.comment_resolved_datetime,
  b.comment_resolution_text,
  b.effective_start_datetime,
  b.effective_end_datetime,
  b.active_indicator,
  b.integrate_insert_datetime,
  b.integrate_insert_batch_number,
  b.integrate_update_datetime,
  b.integrate_update_batch_number,
  b.adw_row_hash
  )
WHEN MATCHED THEN UPDATE SET
  a.effective_start_datetime = b.effective_start_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number
"""

######################################################################
# Load Employee Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

employee_work_source = f"""
-- employee_work_source
-- Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

create or replace table `{int_u.INTEGRATION_PROJECT}.adw_work.dim_employee_source` as
SELECT
'E' as employee_type_code,
cx_iusers.username,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_email`(cx_iusers.email) as active_directory_email_adw_key,
'' as employee_hr_identifier
,'' as employee_hr_hire_date
,'' as employee_hr_termination_date
,'' as employee_hr_termination_reason
,'' as employee_hr_status
,'' as employee_hr_region
,'' as employee_hr_location
,'' as employee_hr_title
,'' as employee_hr_supervisor
,'' as fax_phone_adw_key
,'' as phone_adw_key
,'' as name_adw_key
,'' as email_adw_key
,'' as employee_hr_job_code
,'' as employee_hr_cost_center
,'' as employee_hr_position_type,
ROW_NUMBER() OVER (PARTITION BY cx_iusers.username ORDER BY CURRENT_DATE() DESC) AS dupe_check
FROM `{iu.INGESTION_PROJECT}.mzp.cx_iusers` cx_iusers

"""

######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

employee_work_transformed = f"""
-- employee_work_transformed
-- Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_work_transformed` AS
SELECT employee_type_code,
username,
active_directory_email_adw_key,
employee_hr_identifier,
employee_hr_hire_date,
employee_hr_termination_date,
employee_hr_termination_reason,
employee_hr_status,
employee_hr_region,
employee_hr_location,
employee_hr_title,
employee_hr_supervisor,
fax_phone_adw_key,
phone_adw_key,
name_adw_key,
email_adw_key,
employee_hr_job_code,
employee_hr_cost_center,
employee_hr_position_type,
dupe_check,
CURRENT_DATETIME() AS last_upd_dt,
TO_BASE64(MD5(COALESCE(username,''))) as adw_row_hash
from `{int_u.INTEGRATION_PROJECT}.adw_work.dim_employee_source`
WHERE dupe_check =1
"""

######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

employee_work_final_staging = f"""
-- employee_work_final_staging
-- Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.employee_work_stage` AS
SELECT
COALESCE(target.employee_adw_key ,GENERATE_UUID()) as employee_adw_key,
source.employee_type_code,
source.username as employee_active_directory_user_identifier,
source.active_directory_email_adw_key,
source.employee_hr_identifier,
SAFE_CAST(source.employee_hr_hire_date as DATE) as employee_hr_hire_date,
SAFE_CAST(source.employee_hr_termination_date as DATE) as employee_hr_termination_date,
source.employee_hr_termination_reason,
source.employee_hr_status,
source.employee_hr_region,
source.employee_hr_location,
source.employee_hr_title,
source.employee_hr_supervisor,
source.fax_phone_adw_key,
source.phone_adw_key,
source.name_adw_key,
source.email_adw_key,
source.employee_hr_job_code,
source.employee_hr_cost_center,
source.employee_hr_position_type,
PARSE_DATETIME("%Y-%m-%d %H:%M:%S",'2010-01-01 00:00:00') AS effective_start_datetime,
PARSE_DATETIME("%Y-%m-%d %H:%M:%S",'9999-12-31 00:00:00') AS effective_end_datetime,
'Y' AS active_indicator,
  source.adw_row_hash,
CURRENT_DATETIME() AS integrate_insert_datetime,
1 as integrate_insert_batch_number,
CURRENT_DATETIME() AS integrate_update_datetime,
1 as integrate_update_batch_number
from
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_work_transformed` source
left  join
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` target
on (source.username=target.employee_active_directory_user_identifier
and target.active_indicator='Y')
where  target.employee_adw_key IS NULL
 OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
employee_adw_key,
target.employee_type_code,
target.employee_active_directory_user_identifier,
target.active_directory_email_adw_key,
target.employee_hr_identifier ,
target.employee_hr_hire_date,
target.employee_hr_termination_date,
target.employee_hr_termination_reason,
target.employee_hr_status,
target.employee_hr_region,
target.employee_hr_location,
target.employee_hr_title,
target.employee_hr_supervisor,
target.fax_phone_adw_key,
target.phone_adw_key,
target.name_adw_key,
target.email_adw_key,
target.employee_hr_job_code,
target.employee_hr_cost_center,
target.employee_hr_position_type,
target.effective_start_datetime,
DATETIME_SUB(source.last_upd_dt, INTERVAL 1 SECOND) AS effective_end_datetime,
 'N' AS active_indicator,
   target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
  FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_work_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` target
ON
  (source.username=target.employee_active_directory_user_identifier
    AND target.active_indicator='Y')
 WHERE  source.adw_row_hash <> target.adw_row_hash
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Employee
employee_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.employee_work_stage` b
ON a.employee_adw_key=b.employee_adw_key
when not matched then INSERT(
employee_adw_key,
active_directory_email_adw_key,
email_adw_key,
phone_adw_key,
fax_phone_adw_key,
name_adw_key,
employee_type_code,
employee_active_directory_user_identifier,
employee_hr_identifier,
employee_hr_hire_date	,
employee_hr_termination_date,
employee_hr_termination_reason,
employee_hr_status,
employee_hr_region,
employee_hr_location,
employee_hr_title,
employee_hr_supervisor,
employee_hr_job_code,
employee_hr_cost_center,
employee_hr_position_type,
effective_start_datetime,
effective_end_datetime,
active_indicator,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
,adw_row_hash
)
VALUES(
b.employee_adw_key,
b.active_directory_email_adw_key,
b.email_adw_key,
b.phone_adw_key,
b.fax_phone_adw_key,
b.name_adw_key,
b.employee_type_code,
b.employee_active_directory_user_identifier,
b.employee_hr_identifier,
b.employee_hr_hire_date	,
b.employee_hr_termination_date,
b.employee_hr_termination_reason,
b.employee_hr_status,
b.employee_hr_region,
b.employee_hr_location,
b.employee_hr_title,
b.employee_hr_supervisor,
b.employee_hr_job_code,
b.employee_hr_cost_center,
b.employee_hr_position_type,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.integrate_insert_datetime	,
b.integrate_insert_batch_number	,
b.integrate_update_datetime	,
b.integrate_update_batch_number
, b.adw_row_hash
)
WHEN MATCHED THEN UPDATE
SET
a.effective_end_datetime = b.effective_end_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number

"""

######################################################################
# Load Employee_role Current set of Data
######################################################################


######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


employee_role_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_source` AS
SELECT
  employee_role_source.agent_id,
  employee_role_source.branch_ky,
  employee_role_source.first_name,
  employee_role_source.last_name,
  employee_role_source.user_id,
  employee_role_source.agent_type_cd,
  employee_role_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY employee_role_source.agent_id ORDER BY employee_role_source.last_upd_dt DESC) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.sales_agent` AS employee_role_source
WHERE
  CAST(employee_role_source.last_upd_dt AS datetime) > (
  SELECT
    COALESCE(MAX(effective_start_datetime),'1900-01-01 12:00:00')
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role`)
"""

######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

employee_role_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_transformed` AS
SELECT
  COALESCE(emp.employee_adw_key,
    "-1") AS employee_adw_key,
  COALESCE(name1.name_adw_key,
    TO_BASE64(MD5(CONCAT(ifnull(employee_role.first_name,
            ''),'|',ifnull(employee_role.last_name,
            ''))))) AS name_adw_key,
  COALESCE(office.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  'Membership' AS employee_role_business_line_code,
  employee_role.agent_id AS employee_role_id,
  employee_role.agent_type_cd AS employee_role_type,
  employee_role.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT(ifnull(employee_role.agent_id,
          ''),'|',ifnull(employee_role.agent_type_cd,
          '')))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_source` AS employee_role
LEFT OUTER JOIN(select 
     user_id, USERNAME,
     ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY null ) AS dupe_check
   from `{iu.INGESTION_PROJECT}.mzp.cx_iusers`) users
ON
  employee_role.user_id=users.user_id  and  users.dupe_check=1
LEFT OUTER JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` emp
ON
  emp. employee_active_directory_user_identifier =users.username
  AND emp.active_indicator='Y'
LEFT OUTER JOIN
 ( SELECT
    branch_ky,branch_cd,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_KY ORDER BY LAST_UPD_DT DESC) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.branch`  ) branch
ON
  employee_role.branch_ky=branch.branch_ky and branch.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    membership_branch_code,
    active_indicator,
    ROW_NUMBER() OVER(PARTITION BY membership_branch_code ORDER BY effective_start_datetime DESC) AS dupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` ) office
ON
  branch.branch_cd =office.membership_branch_code
  AND office.dupe_check = 1
  AND office.active_indicator='Y'
LEFT OUTER JOIN (
  SELECT
    name.name_adw_key,
    name.raw_first_name,
    name. raw_last_name,
    xref.effective_start_datetime,
    ROW_NUMBER() OVER(PARTITION BY name. raw_first_name, name. raw_last_name ORDER BY xref.effective_start_datetime DESC) AS dupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name` name,
    `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name` xref
  WHERE
    name.name_adw_key=xref.name_adw_key) name1
ON
  name1. raw_first_name = employee_role.first_name
  AND name1. raw_last_name = employee_role.last_name
  AND name1.dupe_check=1
WHERE
  employee_role.dupe_check=1

"""

######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

employee_role_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_stage` AS
SELECT
  COALESCE(target.employee_role_adw_key,
    GENERATE_UUID()) employee_role_adw_key,
  COALESCE(source.employee_adw_key,
    "-1") AS employee_adw_key,
  source.name_adw_key AS name_adw_key,
  COALESCE(source.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  source.employee_role_business_line_code AS employee_role__business_line_code,
  source.employee_role_id AS employee_role_id,
  source.employee_role_type AS employee_role_type,
  SAFE_CAST(source.effective_start_datetime AS DATETIME) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  1 integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  1 integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role` target
ON
  (source.employee_role_type=target.employee_role_type
    AND target.active_indicator='Y')
WHERE
  target.employee_role_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.employee_role_adw_key,
  target.employee_adw_key,
  target.name_adw_key,
  target.aca_office_adw_key,
  target.employee_role__business_line_code,
  target.employee_role_id,
  target.employee_role_type,
  target.effective_start_datetime,
  DATETIME_SUB(CAST(source.effective_start_datetime AS datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role` target
ON
  (source.employee_role_type=target.employee_role_type
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Employee_role
employee_role_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
 MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_employee_role_work_stage` b
ON
  (a. employee_role_adw_key = b. employee_role_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  employee_role_adw_key , 
  employee_adw_key , 
  name_adw_key , 
  aca_office_adw_key, 
  employee_role__business_line_code , 
  employee_role_id , 
  employee_role_type , 
  effective_start_datetime, 
  effective_end_datetime, 
  active_indicator, 
  adw_row_hash, 
  integrate_insert_datetime, 
  integrate_insert_batch_number, 
  integrate_update_datetime, 
  integrate_update_batch_number ) VALUES (
  b.employee_role_adw_key , 
  b.employee_adw_key , 
  b.name_adw_key , 
  b.aca_office_adw_key, 
  b.employee_role__business_line_code , 
  b.employee_role_id , 
  b.employee_role_type , 
  b.effective_start_datetime, 
  b.effective_end_datetime, 
  b.active_indicator, 
  b.adw_row_hash, 
  b.integrate_insert_datetime, 
  b.integrate_insert_batch_number, 
  b.integrate_update_datetime, 
  b.integrate_update_batch_number
)
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number

"""

####################################################################################################################################
# Membership segments
####################################################################################################################################

membership_segment_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_source`  as 
SELECT 
segmentation_history_ky
,membership_ky
,historysource.segmentation_setup_ky
,membership_exp_dt
,historysource.segmentation_test_end_dt
,historysource.segmentation_schedule_dt
,panel_cd
,segmentation_panel_cd
,control_panel_fl
,commission_cd
,setupsource.name as name
,historysource.adw_lake_insert_datetime
,historysource.adw_lake_insert_batch_number
,historysource.adw_lake_insert_datetime as last_upd_dt
,ROW_NUMBER() OVER(PARTITION BY historysource.segmentation_history_ky ORDER BY historysource.membership_exp_dt DESC) AS dupe_check
FROM `{iu.INGESTION_PROJECT}.mzp.segmentation_history` as historysource 
JOIN (select 
      *,
      row_number() over (partition by name order by segmentation_test_end_dt desc) as rn	  
      from `{iu.INGESTION_PROJECT}.mzp.segmentation_setup` ) setupsource on (historysource.segmentation_setup_ky=setupsource.segmentation_setup_ky and setupsource.rn=1)
WHERE CAST(historysource.adw_lake_insert_datetime AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation`)

"""

membership_segment_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` as 
SELECT 
  segmentation_history_ky
 ,membership.membership_adw_key as membership_adw_key
 ,SAFE_CAST(membership_exp_dt AS date)  as member_expiration_date
 ,panel_cd as segmentation_test_group_indicator
 ,segmentation_panel_cd as segmentation_panel_code
 ,control_panel_fl as  segmentation_control_panel_indicator
 ,commission_cd as segmentation_commission_code
 ,name as segment_name
 ,  concat(ifnull(membership_ky,''),'|', SAFE_CAST(SAFE_CAST(source.membership_exp_dt AS date) AS STRING), '|', ifnull(name,'')) as segment_key
 ,  SAFE_CAST(last_upd_dt as datetime) as effective_start_datetime
  , CAST('9999-12-31' AS datetime) effective_end_datetime
  , 'Y' as active_indicator
  , CURRENT_DATETIME() as integrate_insert_datetime
  , 1 as integrate_insert_batch_number
  , CURRENT_DATETIME() as integrate_update_datetime
  , 1 as integrate_update_batch_number
  , TO_BASE64(MD5(CONCAT(COALESCE(panel_cd,''),
                         COALESCE(segmentation_panel_cd,''),
                         COALESCE(control_panel_fl,''),
                         COALESCE(commission_cd,'')))) as adw_row_hash 
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_source` as source 
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership
ON
  (source.membership_ky=SAFE_CAST(membership.membership_source_system_key as STRING) AND membership.active_indicator='Y')
WHERE source.dupe_check=1 

"""
membership_segment_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_stage` AS
SELECT
  COALESCE(target.membership_marketing_segmentation_adw_key, GENERATE_UUID()) membership_marketing_segmentation_adw_key
  ,source.membership_adw_key
  ,source.member_expiration_date
  ,source.segmentation_test_group_indicator
  ,source.segmentation_panel_code
  ,source.segmentation_control_panel_indicator
  ,source.segmentation_commission_code
  ,source.segment_name
  ,source.adw_row_hash
  ,source.effective_start_datetime
  ,source.effective_end_datetime
  ,source.active_indicator
  ,source.integrate_insert_datetime
  ,source.integrate_insert_batch_number
  ,source.integrate_update_datetime
  ,source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation` target
ON
  (source.segment_key=concat(ifnull(target.membership_adw_key,''),'|', SAFE_CAST(SAFE_CAST(target.member_expiration_date AS date) AS STRING), '|', ifnull(target.segment_name,'')) 
    AND target.active_indicator='Y')
WHERE
  target.membership_marketing_segmentation_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
  target.membership_marketing_segmentation_adw_key
  ,target.membership_adw_key
  ,target.member_expiration_date
  ,target.segmentation_test_group_indicator
  ,target.segmentation_panel_code
  ,target.segmentation_control_panel_indicator
  ,target.segmentation_commission_code
  ,target.segment_name
  ,target.adw_row_hash
  ,target.effective_start_datetime
  ,DATETIME_SUB(SAFE_CAST(source.effective_start_datetime as datetime),
    INTERVAL 1 day) AS effective_end_datetime
  ,'N' AS active_indicator
  ,target.integrate_insert_datetime
  ,target.integrate_insert_batch_number
  ,CURRENT_DATETIME()
  ,1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation` target
ON
  (source.segment_key=concat(ifnull(target.membership_adw_key,''),'|', SAFE_CAST(SAFE_CAST(target.member_expiration_date AS date) AS STRING), '|', ifnull(target.segment_name,'')) 
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash
"""

membership_segment_merge = f"""
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_segment_stage` b
  ON
    (a.membership_marketing_segmentation_adw_key = b.membership_marketing_segmentation_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( 
    membership_marketing_segmentation_adw_key 
    ,membership_adw_key
    ,member_expiration_date
    ,segmentation_test_group_indicator
    ,segmentation_panel_code
    ,segmentation_control_panel_indicator
    ,segmentation_commission_code
    ,segment_name
    ,adw_row_hash
    ,effective_start_datetime
    ,effective_end_datetime
    ,active_indicator
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
   ) VALUES (
     b.membership_marketing_segmentation_adw_key
    ,b.membership_adw_key
    ,b.member_expiration_date
    ,b.segmentation_test_group_indicator
    ,b.segmentation_panel_code
    ,b.segmentation_control_panel_indicator
    ,b.segmentation_commission_code
    ,b.segment_name
    ,b.adw_row_hash
    ,b.effective_start_datetime
    ,b.effective_end_datetime
    ,b.active_indicator
    ,b.integrate_insert_datetime
    ,b.integrate_insert_batch_number
    ,b.integrate_update_datetime
    ,b.integrate_update_batch_number
  )
"""

####################################################################################################################################
# member_auto_renewal_card_key
####################################################################################################################################

#################################
# member_auto_renewal_card_key - Stage Load
#################################
# member_auto_renewal_card_key

######################################################################
# Load member_auto_renewal_card_key Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

member_auto_renewal_card_work_source = f"""
CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_source` AS
SELECT
cc_first_name,
cc_last_name,
CC_STREET ,
CC_City ,
CC_State,
CC_Zip,
AUTORENEWAL_CARD_KY  as member_source_arc_key ,
STATUS_DT as member_arc_status_datetime,
CC_TYPE_CD as  member_cc_type_code,
CC_EXPIRATION_DT as member_cc_expiration_date ,
CC_REJECT_REASON_CD as member_cc_reject_reason_code,
CC_REJECT_DT as member_cc_reject_date,
DONOR_NR as member_cc_donor_number,
CC_LAST_FOUR as member_cc_last_four,
CREDIT_DEBIT_TYPE as member_credit_debit_type_code,	
CURRENT_DATETIME() as last_upd_dt,  
adw_lake_insert_datetime,
ROW_NUMBER() OVER (PARTITION BY AUTORENEWAL_CARD_KY ORDER BY autorenewal.STATUS_DT DESC ) AS DUP_CHECK
  FROM
    `{iu.INGESTION_PROJECT}.mzp.autorenewal_card` autorenewal
      WHERE
  adw_lake_insert_datetime > (  SELECT MAX(effective_start_datetime) FROM `{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` )

"""
####################################################################################################################################################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

member_auto_renewal_card_work_transformed = f"""
CREATE OR REPLACE TABLE
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_transformed` AS
SELECT
`{int_u.INTEGRATION_PROJECT}.udfs.hash_name`(autorenewal.cc_first_name,'',autorenewal.cc_last_name,'','') as name_adw_key,
`{int_u.INTEGRATION_PROJECT}.udfs.hash_address`(autorenewal.CC_STREET,'','',autorenewal.CC_City,autorenewal.CC_State,autorenewal.CC_Zip,'','') as address_adw_key,
member_source_arc_key ,
member_arc_status_datetime,
member_cc_type_code,
member_cc_expiration_date ,
member_cc_reject_reason_code,
member_cc_reject_date,
member_cc_donor_number,
member_cc_last_four,
member_credit_debit_type_code,
CAST (autorenewal.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(
COALESCE(member_source_arc_key ,''),COALESCE(member_arc_status_datetime,''),
COALESCE(member_cc_type_code,''),
COALESCE(member_cc_expiration_date ,''),
COALESCE(member_cc_reject_reason_code,''),
COALESCE(member_cc_reject_date,''),
COALESCE(member_cc_donor_number,''),
COALESCE(member_cc_last_four,''),
COALESCE(member_credit_debit_type_code,'')
))) as adw_row_hash
,
 ROW_NUMBER() OVER (PARTITION BY member_source_arc_key ORDER BY last_upd_dt DESC ) AS DUP_CHECK
FROM
 `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_source` autorenewal where autorenewal.dup_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

member_auto_renewal_card_final_stage = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_final_stage` AS
SELECT
    COALESCE(target. member_auto_renewal_card_adw_key ,GENERATE_UUID()) member_auto_renewal_card_adw_key ,
SAFE_CAST(source.name_adw_key as STRING) name_adw_key,	
SAFE_CAST(source.address_adw_key as STRING) address_adw_key,	
SAFE_CAST(source.member_source_arc_key as INT64) member_source_arc_key ,	
SAFE_CAST(source.member_arc_status_datetime as DATETIME) member_arc_status_datetime,
source.member_cc_type_code,
SAFE_CAST(source.member_cc_expiration_date as DATE) member_cc_expiration_date,
source.member_cc_reject_reason_code,
SAFE_CAST(source.member_cc_reject_date as DATE) member_cc_reject_date,
source.member_cc_donor_number,
source.member_cc_last_four,
source.member_credit_debit_type_code,
last_upd_dt AS effective_start_datetime,
CAST('9999-12-31' AS datetime) effective_end_datetime,
'Y' AS active_indicator,
source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
from 
 `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` target
  ON
    (SAFE_CAST(source.member_source_arc_key as INT64)=target.member_source_arc_key
      AND target.active_indicator='Y')
  WHERE
    target.member_source_arc_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
target.member_auto_renewal_card_adw_key	,
target.name_adw_key	,		
target.address_adw_key	,		
target.member_source_arc_key ,	
target.member_arc_status_datetime,
target.member_cc_type_code,
target.member_cc_expiration_date ,
target.member_cc_reject_reason_code,
target.member_cc_reject_date,
target.member_cc_donor_number,
target.member_cc_last_four,
target.member_credit_debit_type_code,   
target.effective_start_datetime,
DATETIME_SUB(source.last_upd_dt,
INTERVAL 1 day) AS effective_end_datetime,
'N' AS active_indicator,
target.adw_row_hash,
target.integrate_insert_datetime,
target.integrate_insert_batch_number,
CURRENT_DATETIME(),
1
FROM
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_work_transformed` source
JOIN
`{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` target
ON
(SAFE_CAST(source.member_source_arc_key as  INT64)=target.member_source_arc_key
AND target.active_indicator='Y')
WHERE
source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# merge_member_auto_renewal_card
merge_member_auto_renewal_card = f"""

MERGE INTO
   `{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` a
  USING
     `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_auto_renewal_card_final_stage` b
  ON
    (a.member_source_arc_key = b.member_source_arc_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT (
member_auto_renewal_card_adw_key		,
name_adw_key			            ,
address_adw_key			            ,
member_source_arc_key		        ,
member_arc_status_datetime		    ,
member_cc_type_code			        ,
member_cc_expiration_date		    ,
member_cc_reject_reason_code		,	
member_cc_reject_date		        ,
member_cc_donor_number			    ,
member_cc_last_four			        ,
member_credit_debit_type_code		,	
effective_start_datetime		    ,
effective_end_datetime		        ,
active_indicator			        ,
adw_row_hash			            ,
integrate_insert_datetime		    ,
integrate_insert_batch_number		,
integrate_update_datetime		    ,
integrate_update_batch_number)
VALUES (
b.member_auto_renewal_card_adw_key		,
b.name_adw_key			            ,
b.address_adw_key			            ,
b.member_source_arc_key		        ,
b.member_arc_status_datetime		    ,
b.member_cc_type_code			        ,
b.member_cc_expiration_date		    ,
b.member_cc_reject_reason_code		,	
b.member_cc_reject_date		        ,
b.member_cc_donor_number			    ,
b.member_cc_last_four			        ,
b.member_credit_debit_type_code		,	
b.effective_start_datetime		    ,
b.effective_end_datetime		        ,
b.active_indicator			        ,
b.adw_row_hash			            ,
b.integrate_insert_datetime		    ,
b.integrate_insert_batch_number		,
b.integrate_update_datetime		    ,
b.integrate_update_batch_number		 )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number

"""

####################################################################################################################################
# Billing_Summary
####################################################################################################################################

# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

membership_billing_summary_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_source` AS
 SELECT
billing_summary_source.bill_summary_ky as bill_summary_source_key,
billing_summary_source.membership_ky ,
billing_summary_source.process_dt as bill_process_date,
billing_summary_source.notice_nr as bill_notice_number,
billing_summary_source.bill_at as bill_amount,
billing_summary_source.credit_at as bill_credit_amount,
billing_summary_source.bill_type,
billing_summary_source.expiration_dt as member_expiration_date,
billing_summary_source.payment_at as bill_paid_amount,
billing_summary_source.renew_method_cd as bill_renewal_method,
billing_summary_source.bill_panel as bill_panel_code,
billing_summary_source.ebill_fl as bill_ebilling_indicator,
billing_summary_source.last_upd_dt,
ROW_NUMBER() OVER(PARTITION BY billing_summary_source.bill_summary_ky ORDER BY billing_summary_source.last_upd_dt DESC) AS dupe_check
  FROM
  `{iu.INGESTION_PROJECT}.mzp.bill_summary` AS billing_summary_source
WHERE
  billing_summary_source.last_upd_dt IS NOT NULL AND
  CAST(billing_summary_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary`)


"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_billing_summary_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` AS
 SELECT
 adw_memship.membership_adw_key as membership_adw_key,
 billing_summary_source.bill_summary_source_key,
 billing_summary_source.bill_process_date,
 billing_summary_source.bill_notice_number,
 billing_summary_source.bill_amount,
 billing_summary_source.bill_credit_amount,
 billing_summary_source.bill_type,
 billing_summary_source.member_expiration_date,
 billing_summary_source.bill_paid_amount,
 billing_summary_source.bill_renewal_method,
 billing_summary_source.bill_panel_code,
 billing_summary_source.bill_ebilling_indicator,
 CAST (billing_summary_source.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(adw_memship.membership_adw_key,''),'|',
ifnull(billing_summary_source.bill_summary_source_key,''),'|',
ifnull(billing_summary_source.bill_process_date,''),'|',
ifnull(billing_summary_source.bill_notice_number,''),'|',
ifnull(billing_summary_source.bill_amount,''),'|',
ifnull(billing_summary_source.bill_credit_amount,''),'|',
ifnull(billing_summary_source.bill_type,''),'|',
ifnull(billing_summary_source.member_expiration_date,''),'|',
ifnull(billing_summary_source.bill_paid_amount,''),'|',
ifnull(billing_summary_source.bill_renewal_method,''),'|',
ifnull(billing_summary_source.bill_panel_code,''),'|',
ifnull(billing_summary_source.bill_ebilling_indicator,''),'|'
))) as adw_row_hash

FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_source` AS billing_summary_source
    LEFT JOIN
     `{int_u.INTEGRATION_PROJECT}.member.dim_membership` adw_memship
 on billing_summary_source.membership_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING) AND active_indicator='Y'
 --where dupe_check = 1


"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_summary_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_final_stage` AS
SELECT
COALESCE(target.membership_billing_summary_adw_key,GENERATE_UUID()) AS membership_billing_summary_adw_key,
source.membership_adw_key AS membership_adw_key,
SAFE_CAST(source.bill_summary_source_key AS INT64) AS bill_summary_source_key,
CAST(substr (source.bill_process_date,0,10) AS Date) AS bill_process_date,
CAST(source.bill_notice_number AS INT64) AS bill_notice_number,
CAST(source.bill_amount AS NUMERIC) AS bill_amount,
CAST(source.bill_credit_amount AS NUMERIC) AS bill_credit_amount,
source.bill_type,
CAST(substr (source.member_expiration_date,0,10) AS Date) AS member_expiration_date,
CAST(source.bill_paid_amount AS NUMERIC) AS bill_paid_amount,
source.bill_renewal_method,
source.bill_panel_code,
source.bill_ebilling_indicator,
source.last_upd_dt AS effective_start_datetime,
CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number

 FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` target
  ON
    (source.bill_summary_source_key =SAFE_CAST(target.bill_summary_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_billing_summary_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
target.membership_billing_summary_adw_key,
target.membership_adw_key,
target.bill_summary_source_key,
target.bill_process_date,
target.bill_notice_number,
target.bill_amount,
target.bill_credit_amount,
target.bill_type,
target.member_expiration_date,
target.bill_paid_amount,
target.bill_renewal_method,
target.bill_panel_code,
target.bill_ebilling_indicator,
target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` target
  ON
    (source.bill_summary_source_key =SAFE_CAST(target.bill_summary_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# billing_summary
membership_billing_summary_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_summary_work_final_stage` b
    ON (a.membership_billing_summary_adw_key = b.membership_billing_summary_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
membership_billing_summary_adw_key,
membership_adw_key,
bill_summary_source_key,
bill_process_date,
bill_notice_number,
bill_amount,
bill_credit_amount,
bill_type,
member_expiration_date,
bill_paid_amount,
bill_renewal_method,
bill_panel_code,
bill_ebilling_indicator,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
    ) VALUES
    (
b.membership_billing_summary_adw_key,
COALESCE(b.membership_adw_key,'-1'),
b.bill_summary_source_key,
b.bill_process_date,
b.bill_notice_number,
b.bill_amount,
b.bill_credit_amount,
b.bill_type,
b.member_expiration_date,
b.bill_paid_amount,
b.bill_renewal_method,
b.bill_panel_code,
b.bill_ebilling_indicator,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number
    )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number

"""

####################################################################################################################################
# Billing_detail
####################################################################################################################################
######################################################################
# Load Billing_detail Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

membership_billing_detail_work_source = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_source` AS
 select billing_detail_source.bill_detail_ky as bill_detail_source_key,
 billing_detail_source.bill_summary_ky as bill_summary_source_key,
 billing_detail_source.member_ky ,
 billing_detail_source.rider_ky ,
 billing_detail_source.membership_fees_ky,
 billing_detail_source.bill_detail_at as bill_detail_amount,
 billing_detail_source.detail_text as bill_detail_text,
 billing_detail_source.billing_category_cd as rider_billing_category_code,
 billing_detail_source.solicitation_cd as rider_solicitation_code,
 billing_detail_source.last_upd_dt,
 ROW_NUMBER() OVER(PARTITION BY billing_detail_source.bill_detail_ky ORDER BY billing_detail_source.last_upd_dt DESC) AS dupe_check
   FROM
   `{iu.INGESTION_PROJECT}.mzp.bill_detail` AS billing_detail_source
WHERE
  billing_detail_source.last_upd_dt IS NOT NULL AND
  CAST(billing_detail_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail`)


"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_billing_detail_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` AS
  SELECT
 billing_summary.membership_billing_summary_adw_key,
 billing_summary.membership_adw_key,
 adw_mem.member_adw_key,
 adw_product.product_adw_key,
 adw_membership_fee.membership_fee_adw_key,
 billing_detail_source.bill_detail_source_key,
 billing_detail_source.bill_summary_source_key,
 billing_detail_source.bill_detail_amount,
 billing_summary.bill_process_date, 
 billing_summary.bill_notice_number,
 billing_summary.bill_type,
 billing_detail_source.bill_detail_text,
 billing_detail_source.rider_billing_category_code,
 adw_cx_codes.code_desc as rider_billing_category_description,
 billing_detail_source.rider_solicitation_code,
 CAST (billing_detail_source.last_upd_dt AS datetime) AS last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(billing_summary.membership_adw_key,''),'|',
ifnull(adw_mem.member_adw_key,''),'|',
ifnull(adw_membership_fee.membership_fee_adw_key,''),'|',
ifnull(billing_detail_source.bill_detail_source_key,''),'|',
ifnull(billing_detail_source.bill_summary_source_key,''),'|',
ifnull(billing_detail_source.bill_detail_amount,''),'|',
ifnull(CAST(billing_summary.bill_process_date AS STRING),''),'|',
ifnull(CAST(billing_summary.bill_notice_number AS STRING),''),'|',
ifnull(billing_summary.bill_type,''),'|',
ifnull(billing_detail_source.bill_detail_text,''),'|',
ifnull(billing_detail_source.rider_billing_category_code,''),'|',
ifnull(billing_detail_source.rider_solicitation_code,''),'|',
ifnull(adw_cx_codes.code_desc,''),'|',
ifnull(billing_detail_source.rider_solicitation_code,''),'|'
))) as adw_row_hash

FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_source` AS billing_detail_source LEFT JOIN 
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` AS billing_summary 
                                on billing_detail_source.bill_summary_source_key =SAFE_CAST(billing_summary.bill_summary_source_key AS STRING) AND active_indicator='Y' LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` adw_mem
                                on billing_detail_source.member_ky =SAFE_CAST(adw_mem.member_source_system_key AS STRING) AND adw_mem.active_indicator='Y' LEFT JOIN
    (SELECT rider_ky, rider_comp_cd , ROW_NUMBER() OVER(PARTITION BY rider_ky ORDER BY last_upd_dt DESC) AS latest_rec
     FROM `{iu.INGESTION_PROJECT}.mzp.rider`) adw_rider
                                on billing_detail_source.rider_ky = adw_rider.rider_ky and adw_rider.latest_rec = 1 LEFT JOIN
    (select a.product_adw_key, a.product_sku_description
     from `{int_u.INTEGRATION_PROJECT}.adw.dim_product` a JOIN `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` b
     on a.product_category_adw_key = b.product_category_adw_key
     where b.product_category_code = 'RCC' 
     AND a.active_indicator='Y'
     AND b.active_indicator='Y') adw_product 
                                on adw_rider.rider_comp_cd = adw_product.product_sku_description LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` adw_membership_fee
                                on billing_detail_source.membership_fees_ky =SAFE_CAST(adw_membership_fee.membership_fee_source_key AS STRING) AND adw_membership_fee.active_indicator='Y'  LEFT JOIN 
    (SELECT code, code_desc, ROW_NUMBER() OVER(PARTITION BY code_ky ORDER BY last_upd_dt DESC) AS latest_rec
     FROM `{iu.INGESTION_PROJECT}.mzp.cx_codes` 
     where code_type = 'BILTYP') adw_cx_codes
                                on billing_detail_source.rider_billing_category_code =adw_cx_codes.code AND adw_cx_codes.latest_rec = 1
 --where dupe_check = 1


"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_billing_detail_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_final_stage` AS
SELECT
COALESCE(target.membership_billing_detail_adw_key,GENERATE_UUID()) AS membership_billing_detail_adw_key,
source.membership_billing_summary_adw_key,
COALESCE(source.member_adw_key,'-1') as member_adw_key,
COALESCE(source.product_adw_key,'-1') as product_adw_key,
SAFE_CAST(source.bill_detail_source_key AS INT64) as bill_detail_source_key,
SAFE_CAST(source.bill_summary_source_key AS INT64) as bill_summary_source_key,
CAST(source.bill_process_date AS Date) as bill_process_date,
CAST(source.bill_notice_number AS INT64) as bill_notice_number,
source.bill_type,
CAST(source.bill_detail_amount AS NUMERIC) as bill_detail_amount,
source.bill_detail_text,
source.rider_billing_category_code,
source.rider_billing_category_description,
source.rider_solicitation_code,
source.last_upd_dt AS effective_start_datetime,
CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number

 FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail` target
  ON
    (source.bill_detail_source_key =SAFE_CAST(target.bill_detail_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_billing_detail_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
target.membership_billing_detail_adw_key,
target.membership_billing_summary_adw_key,
target.member_adw_key,
target.product_adw_key,
target.bill_detail_source_key,
target.bill_summary_source_key,
target.bill_process_date,
target.bill_notice_number,
target.bill_type,
target.bill_detail_amount,
target.bill_detail_text,
target.rider_billing_category_code,
target.rider_billing_category_description,
target.rider_solicitation_code,
target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail` target
  ON
    (source.bill_detail_source_key =SAFE_CAST(target.bill_detail_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# billing_detail
membership_billing_detail_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.membership_billing_detail_work_final_stage` b
    ON (a.membership_billing_summary_adw_key = b.membership_billing_detail_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
membership_billing_detail_adw_key,
membership_billing_summary_adw_key,
member_adw_key,
product_adw_key,
bill_detail_source_key,
bill_summary_source_key,
bill_process_date,
bill_notice_number,
bill_type,
bill_detail_amount,
bill_detail_text,
rider_billing_category_code,
rider_billing_category_description,
rider_solicitation_code,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
    ) VALUES
    (
b.membership_billing_detail_adw_key,
b.membership_billing_summary_adw_key,
COALESCE(b.member_adw_key,'-1'),
COALESCE(b.product_adw_key,'-1'),
b.bill_detail_source_key,
b.bill_summary_source_key,
b.bill_process_date,
b.bill_notice_number,
b.bill_type,
b.bill_detail_amount,
b.bill_detail_text,
b.rider_billing_category_code,
b.rider_billing_category_description,
b.rider_solicitation_code,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number
    )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
"""

#################################################################
# Payment summary
##################################################################

payment_applied_summary_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_source`  as 
SELECT 
membership_payment_ky
,membership_ky
,payment_dt
,payment_at
,batch_name
,payment_method_cd
,cc_type_cd
,cc_expiration_dt
,transaction_type_cd
,adjustment_description_cd
,source_cd
,create_dt
,paid_by_cd
,cc_authorization_nr
,reason_cd
,check_nr
,check_dt
,rev_membership_payment_ky
,donor_nr
,bill_summary_ky
,pnref
,user_id
,unapplied_at
,parent_payment_ky
,pos_sale_id
,pos_ofc_id
,pos_cust_recpt_nr
,batch_ky
,pos_comment
,pos_consultant_id
,cc_first_name
,cc_middle_name
,cc_last_name
,cc_street
,cc_zip
,ach_bank_name
,ach_bank_account_type
,ach_bank_account_name
,ach_authorization_nr
,ach_street
,ach_city
,ach_state
,ach_zip
,ach_email
,transaction_id
,cc_transaction_id
,advance_pay_at
,ach_token
,transaction_cd
,branch_ky
,cc_last_four
,cc_state
,cc_city
,last_upd_dt
,adw_lake_insert_datetime
,adw_lake_insert_batch_number 
,ROW_NUMBER() OVER(PARTITION BY membership_payment_ky ORDER BY last_upd_dt DESC) AS dupe_check
FROM `{iu.INGESTION_PROJECT}.mzp.payment_summary` 
WHERE CAST(last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary`)

"""

payment_applied_summary_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` as 
SELECT 
 SAFE_CAST(membership_payment_ky as int64)  as payment_applied_source_key 
,COALESCE(aca_office.aca_office_adw_key,'-1')  as aca_office_adw_key
,COALESCE(membership.membership_adw_key,'-1') as membership_adw_key
,SAFE_CAST(payment_dt as date)  as payment_applied_date
,SAFE_CAST(payment_at as numeric)  as payment_applied_amount
,batch_name as payment_batch_name
,payment_method_cd as payment_method_code
,cx_codes1.code_desc as payment_method_description
,transaction_type_cd as payment_type_code
,cx_codes2.code_desc as payment_type_description
,adjustment_description_cd as payment_adjustment_code
,cx_codes3.code_desc as payment_adjustment_description
,SAFE_CAST(create_dt as datetime)  as payment_created_datetime
,CASE paid_by_cd
    WHEN 'P' THEN 'Primary'
    WHEN 'D' THEN 'Donor'
    WHEN 'A' THEN 'Associate'
END AS payment_paid_by_code  
,COALESCE(rev_membership_payment_ky,'')  as membership_payment_applied_reversal_adw_key
,COALESCE(employee.employee_adw_key,'-1')  as employee_adw_key
,SAFE_CAST(unapplied_at as numeric)  as payment_unapplied_amount
,COALESCE(payments_received.membership_payments_received_adw_key,'-1') as membership_payments_received_adw_key
,TO_BASE64(MD5(CONCAT(COALESCE(payment_dt,'')
,COALESCE( payment_at,'')
,COALESCE( payment_dt,'')
,COALESCE( batch_name,'')
,COALESCE( payment_method_cd,'')
,COALESCE( transaction_type_cd,'')
,COALESCE( adjustment_description_cd,'')
,COALESCE( create_dt,'')
,COALESCE( rev_membership_payment_ky,'')
,COALESCE( cx_codes1.code_desc,'')
,COALESCE( payment_type_description,'')
,COALESCE( payment_adjustment_description,'')
,COALESCE( unapplied_at,'')
,COALESCE( membership_payment_ky,'-1')
,COALESCE( membership.membership_adw_key,'-1')
,COALESCE( rev_membership_payment_ky,'-1')
,COALESCE( employee.employee_adw_key,'-1')
))) as adw_row_hash
,SAFE_CAST(source.last_upd_dt AS DATETIME)  as effective_start_datetime
,CAST('9999-12-31' AS datetime) effective_end_datetime
,'Y' as active_indicator
,CURRENT_DATETIME() as integrate_insert_datetime
,1 as integrate_insert_batch_number
,CURRENT_DATETIME() as integrate_update_datetime
,1 as integrate_update_batch_number
FROM `adw-dev.adw_work.mzp_payment_summary_source` as source 
LEFT JOIN
  `{iu.INGESTION_PROJECT}.mzp.cx_codes` cx_codes1 ON (
  source.payment_method_cd=cx_codes1.code_type and cx_codes1.code_type='PEMTH' )
LEFT JOIN
  `{iu.INGESTION_PROJECT}.mzp.cx_codes` cx_codes2 ON (
  source.payment_method_cd=cx_codes2.code_type and cx_codes2.code_type='PAYSRC' )
LEFT JOIN
  `{iu.INGESTION_PROJECT}.mzp.cx_codes` cx_codes3 ON (
  source.payment_method_cd=cx_codes3.code_type and cx_codes3.code_type='PAYADJ' )
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership ON (
  source.membership_ky=SAFE_CAST(membership.membership_source_system_key as STRING) and membership.active_indicator='Y' )
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` payments_received ON (
source.rev_membership_payment_ky=SAFE_CAST(payments_received.payment_received_batch_key as STRING) and payments_received.active_indicator='Y' )
LEFT JOIN
    `{iu.INGESTION_PROJECT}.mzp.branch` as branch ON source.branch_ky = branch.branch_ky
LEFT JOIN `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` as aca_office
    	on branch.branch_cd = aca_office.membership_branch_code and aca_office.active_indicator='Y' 
LEFT JOIN
    (select max(user_id) as user_id, username as username from `{iu.INGESTION_PROJECT}.mzp.cx_iusers` group by username) cx_iusers ON source.user_id = cx_iusers.user_id 
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` employee ON (
cx_iusers.username=SAFE_CAST(employee.employee_active_directory_user_identifier as STRING) and employee.active_indicator ='Y' )
WHERE source.dupe_check=1 
"""

payment_summary_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_source_stage` AS
SELECT
 GENERATE_UUID() as membership_payment_applied_summary_adw_key 
,source.membership_payment_applied_reversal_adw_key
,source.payment_applied_source_key
,source.membership_adw_key
,source.membership_payments_received_adw_key
,source.aca_office_adw_key
,source.employee_adw_key
,source.payment_applied_date
,source.payment_applied_amount
,source.payment_batch_name
,source.payment_method_code
,source.payment_method_description
,source.payment_type_code
,source.payment_type_description
,source.payment_adjustment_code
,source.payment_adjustment_description
,source.payment_created_datetime
,source.payment_paid_by_code
,source.payment_unapplied_amount
,source.effective_start_datetime
,source.effective_end_datetime
,source.active_indicator
,source.adw_row_hash
,source.integrate_insert_datetime
,source.integrate_insert_batch_number
,source.integrate_update_datetime
,source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` target
ON
  (source.payment_applied_source_key=target.payment_applied_source_key
    AND target.active_indicator='Y')
WHERE
  target.membership_payment_applied_summary_adw_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
 membership_payment_applied_summary_adw_key 
,target.membership_payment_applied_reversal_adw_key
,target.payment_applied_source_key
,target.membership_adw_key
,target.membership_payments_received_adw_key
,target.aca_office_adw_key
,target.employee_adw_key
,target.payment_applied_date
,target.payment_applied_amount
,target.payment_batch_name
,target.payment_method_code
,target.payment_method_description
,target.payment_type_code
,target.payment_type_description
,target.payment_adjustment_code
,target.payment_adjustment_description
,target.payment_created_datetime
,target.payment_paid_by_code
,target.payment_unapplied_amount
,target.effective_start_datetime
,DATETIME_SUB(source.effective_start_datetime,
    INTERVAL 1 day) AS effective_end_datetime
,'N' AS active_indicator
,target.adw_row_hash
,target.integrate_insert_datetime
,target.integrate_insert_batch_number
,CURRENT_DATETIME()
,1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` target
ON
  (source.payment_applied_source_key=target.payment_applied_source_key
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""
payment_summary_merge = f"""
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_summary_source_stage` b
  ON
    (a.membership_payment_applied_summary_adw_key = b.membership_payment_applied_summary_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( 
   membership_payment_applied_summary_adw_key 
  ,membership_payment_applied_reversal_adw_key
  ,payment_applied_source_key
  ,membership_adw_key
  ,membership_payments_received_adw_key
  ,aca_office_adw_key
  ,employee_adw_key
  ,payment_applied_date
  ,payment_applied_amount
  ,payment_batch_name
  ,payment_method_code
  ,payment_method_description
  ,payment_type_code
  ,payment_type_description
  ,payment_adjustment_code
  ,payment_adjustment_description
  ,payment_created_datetime
  ,payment_paid_by_code
  ,payment_unapplied_amount
  ,effective_start_datetime
  ,effective_end_datetime
  ,active_indicator
  ,adw_row_hash
  ,integrate_insert_datetime
  ,integrate_insert_batch_number
  ,integrate_update_datetime
  ,integrate_update_batch_number

   ) VALUES (
  b.membership_payment_applied_summary_adw_key 
  ,b.membership_payment_applied_reversal_adw_key
  ,b.payment_applied_source_key
  ,b.membership_adw_key
  ,b.membership_payments_received_adw_key
  ,b.aca_office_adw_key
  ,b.employee_adw_key
  ,b.payment_applied_date
  ,b.payment_applied_amount
  ,b.payment_batch_name
  ,b.payment_method_code
  ,b.payment_method_description
  ,b.payment_type_code
  ,b.payment_type_description
  ,b.payment_adjustment_code
  ,b.payment_adjustment_description
  ,b.payment_created_datetime
  ,b.payment_paid_by_code
  ,b.payment_unapplied_amount
  ,b.effective_start_datetime
  ,b.effective_end_datetime
  ,b.active_indicator
  ,b.adw_row_hash
  ,b.integrate_insert_datetime
  ,b.integrate_insert_batch_number
  ,b.integrate_update_datetime
  ,b.integrate_update_batch_number
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
"""

######################################################################
# Load membership_gl_payments_applied Current set of Data   (SAHIL)
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_gl_payments_applied_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
   `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_source` AS
 SELECT
 membership_gl_pay_app_source.JOURNAL_ENTRY_KY ,
 membership_gl_pay_app_source.GL_ACCOUNT_KY ,
 membership_gl_pay_app_source.membership_ky,
 membership_gl_pay_app_source.POST_DT ,
 membership_gl_pay_app_source.PROCESS_DT ,
 membership_gl_pay_app_source.Journal_at ,
 membership_gl_pay_app_source.CREATE_DT ,
 membership_gl_pay_app_source.DR_CR_IND ,
 membership_gl_pay_app_source.last_upd_dt, 

   ROW_NUMBER() OVER(PARTITION BY membership_gl_pay_app_source.JOURNAL_ENTRY_KY ORDER BY membership_gl_pay_app_source.last_upd_dt DESC) AS dupe_check
 FROM
   `{iu.INGESTION_PROJECT}.mzp.journal_entry` AS membership_gl_pay_app_source
  WHERE
    CAST(membership_gl_pay_app_source.last_upd_dt AS datetime) > (
    SELECT
     MAX(effective_start_datetime)
    FROM
     `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_gl_payments_applied_work_transformed = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` AS
SELECT
  COALESCE(membership.membership_adw_key,
    "-1") AS membership_adw_key,
  COALESCE(office.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  CAST(membership_gl_pay_app_source.JOURNAL_ENTRY_KY AS INT64) AS gl_payment_journal_source_key,
  gl_account.GL_DESCR AS gl_payment_gl_description,
  gl_account.GL_ACCT_NUMBER AS gl_payment_account_number,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.POST_DT,1,10)) AS Date) AS gl_payment_post_date,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.PROCESS_DT,1,10)) AS Date) AS gl_payment_process_date,
  SAFE_CAST(membership_gl_pay_app_source.Journal_at AS NUMERIC) AS gl_payment_journal_amount,
  SAFE_CAST(membership_gl_pay_app_source.CREATE_DT AS DATETIME) AS gl_payment_journal_created_datetime,
  membership_gl_pay_app_source.DR_CR_IND AS gl_payment_journal_debit_credit_indicator,
  membership_gl_pay_app_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(membership.membership_adw_key,
            "-1"),
          ''),'|', ifnull(COALESCE(office.aca_office_adw_key,
            "-1"),
          ''),'|', ifnull(gl_account.GL_DESCR,
          ''),'|', ifnull(gl_account.GL_ACCT_NUMBER,
          ''),'|',ifnull(membership_gl_pay_app_source.POST_DT,
          ''),'|',ifnull(membership_gl_pay_app_source.PROCESS_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.Journal_at,
          ''), '|',ifnull(membership_gl_pay_app_source.CREATE_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.DR_CR_IND,
          '')))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_source` AS membership_gl_pay_app_source
LEFT OUTER JOIN (
  SELECT
    membership_adw_key,
    membership_source_system_key,
    active_indicator
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership`
  WHERE
    active_indicator='Y' ) membership
ON
  CAST(membership.membership_source_system_key AS string)=membership_gl_pay_app_source.JOURNAL_ENTRY_KY
LEFT OUTER JOIN (
  SELECT
    GL_ACCOUNT_KY,
    GL_DESCR,
    GL_ACCT_NUMBER,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY GL_ACCOUNT_KY ORDER BY NULL ) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.gl_account` ) gl_account
ON
  gl_account.GL_ACCOUNT_KY=membership_gl_pay_app_source.GL_ACCOUNT_KY
  AND gl_account.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    BRANCH_CD,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_KY ORDER BY NULL) AS dupe_check
  FROM
    `{iu.INGESTION_PROJECT}.mzp.branch` ) branch
ON
  branch.BRANCH_KY=gl_account.BRANCH_KY
  AND branch.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    membership_branch_code
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office`
  WHERE
    active_indicator='Y') office
ON
  office.membership_branch_code=branch.BRANCH_KY
WHERE
  membership_gl_pay_app_source.dupe_check=1

"""

######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_gl_payments_applied_work_final_staging = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_stage` AS
SELECT
  COALESCE(target.membership_gl_payments_applied_adw_key,
    GENERATE_UUID()) membership_gl_payments_applied_adw_key,
  source.membership_adw_key,
  source. aca_office_adw_key,
  source. gl_payment_journal_source_key AS gl_payment_journal_source_key,
  source. gl_payment_gl_description,
  source. gl_payment_account_number,
  source. gl_payment_post_date AS gl_payment_post_date,
  source. gl_payment_process_date  AS gl_payment_process_date,
  source. gl_payment_journal_amount  AS gl_payment_journal_amount,
  source. gl_payment_journal_created_datetime  AS gl_payment_journal_created_datetime,
  source. gl_payment_journal_debit_credit_indicator AS gl_payment_journal_debit_credit_indicator,
  SAFE_CAST(source.effective_start_datetime AS DATETIME) AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  1 integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  1 integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied` target
ON
  (source.gl_payment_journal_source_key=target.gl_payment_journal_source_key
    AND target.active_indicator='Y')
WHERE
  target.gl_payment_journal_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  target.membership_gl_payments_applied_adw_key,
  target.membership_adw_key,
  target.aca_office_adw_key,
  target.gl_payment_journal_source_key,
  target.gl_payment_gl_description,
  target.gl_payment_account_number,
  target.gl_payment_post_date,
  target.gl_payment_process_date,
  target.gl_payment_journal_amount,
  target.gl_payment_journal_created_datetime,
  target.gl_payment_journal_debit_credit_indicator,
  target.effective_start_datetime,
  DATETIME_SUB(CAST(source.effective_start_datetime AS datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied` target
ON
  (source. gl_payment_journal_source_key =target. gl_payment_journal_source_key
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_gl_payments_applied
membership_gl_payments_applied_merge = f"""

 MERGE INTO
   `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied` a
 USING
   `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_gl_payments_applied_work_stage` b
 ON
   (a.gl_payment_journal_source_key = b.gl_payment_journal_source_key
   AND a.effective_start_datetime = b.effective_start_datetime)
   WHEN NOT MATCHED THEN INSERT ( 
   membership_gl_payments_applied_adw_key , 
   membership_adw_key , 
   aca_office_adw_key , 
   gl_payment_journal_source_key , 
   gl_payment_gl_description , 
   gl_payment_account_number , 
   gl_payment_post_date , 
   gl_payment_process_date ,
   gl_payment_journal_amount ,
   gl_payment_journal_created_datetime ,
   gl_payment_journal_debit_credit_indicator ,
   effective_start_datetime, 
   effective_end_datetime, 
   active_indicator, 
   adw_row_hash, 
   integrate_insert_datetime, 
   integrate_insert_batch_number, 
   integrate_update_datetime, 
   integrate_update_batch_number ) VALUES (
   b.membership_gl_payments_applied_adw_key , 
   b.membership_adw_key , 
   b.aca_office_adw_key , 
   b.gl_payment_journal_source_key , 
   b.gl_payment_gl_description , 
   b.gl_payment_account_number , 
   b.gl_payment_post_date , 
   b.gl_payment_process_date ,
   b.gl_payment_journal_amount ,
   b.gl_payment_journal_created_datetime ,
   b.gl_payment_journal_debit_credit_indicator ,
   b.effective_start_datetime, 
   b.effective_end_datetime, 
   b.active_indicator, 
   b.adw_row_hash, 
   b.integrate_insert_datetime, 
   b.integrate_insert_batch_number, 
   b.integrate_update_datetime, 
   b.integrate_update_batch_number
 )
   WHEN MATCHED
   THEN
 UPDATE
 SET
   a.effective_end_datetime = b.effective_end_datetime,
   a.active_indicator = b.active_indicator,
   a.integrate_update_datetime = b.integrate_update_datetime,
   a.integrate_update_batch_number = b.integrate_update_batch_number

"""

######################################
# payments_received_work_source - Level 0 - Source data
######################################

membership_payments_received_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_payments_received_work_source` AS
  SELECT
  batch_payment.MEMBERSHIP_KY,
  batch_payment.BILL_KY,
  batch_header.BRANCH_KY,
  batch_header.USER_ID,
  batch_payment.AUTORENEWAL_CARD_KY,
  batch_payment.BATCH_KY,
  batch_payment.BATCH_PAYMENT_KY,
  batch_header.BATCH_NAME,
  batch_header.EXP_CT,
  batch_header.EXP_AT,
  batch_payment.CREATE_DT,
  batch_header.STATUS,
  batch_header.STATUS_DT,
  batch_payment.PAYMENT_AT,
  batch_payment.PAYMENT_METHOD_CD,
batch_payment.PAYMENT_METHOD_CD AS payment_received_method_description,
batch_payment.PAYMENT_SOURCE_CD,
batch_payment.PAYMENT_SOURCE_CD AS payment_source_description,
batch_payment.TRANSACTION_TYPE_CD,
batch_payment.TRANSACTION_TYPE_CD AS payment_type_description,
batch_payment.REASON_CD,
CASE WHEN batch_payment.PAID_BY_CD='P' THEN 'Payment' 
     ELSE (CASE WHEN batch_payment.PAID_BY_CD='D' THEN 'Discount' 
                ELSE (CASE WHEN batch_payment.PAID_BY_CD='V' THEN 'Voucher'
                           ELSE NULL
                           END
                      )END
          ) END AS PAID_BY_CD,
batch_payment.ADJUSTMENT_DESCRIPTION_CD,
batch_payment.POST_COMPLETED_FL,
batch_payment.POST_DT,
batch_payment.AUTO_RENEW_FL,
batch_payment.LAST_UPD_DT,
ROW_NUMBER() OVER(PARTITION BY batch_payment.BATCH_PAYMENT_KY,batch_payment.last_upd_dt ORDER BY NULL DESC) AS dupe_check
  FROM `{iu.INGESTION_PROJECT}.mzp.batch_payment` batch_payment,
   `{iu.INGESTION_PROJECT}.mzp.batch_header` batch_header
  where batch_header.BATCH_KY=batch_payment.BATCH_KY
   AND CAST(batch_payment.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership`)
"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_payments_received_work_transformed = f"""
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` AS
SELECT
COALESCE(membership.membership_adw_key,'-1') membership_adw_key,
COALESCE(bill_summary.membership_billing_summary_adw_key,'-1') membership_billing_summary_adw_key,
COALESCE(renewal.member_auto_renewal_card_adw_key,'-1') member_auto_renewal_card_adw_key,
COALESCE(branch1.aca_office_adw_key,'-1') aca_office_adw_key,
COALESCE(users1.employee_adw_key,'-1') employee_adw_key,
payments.BATCH_KY,
payments.BATCH_PAYMENT_KY,
payments.BATCH_NAME,
payments.EXP_CT,
payments.EXP_AT,
payments.CREATE_DT,
payments.STATUS,
payments.STATUS_DT,
payments.PAYMENT_AT,
payments.PAYMENT_METHOD_CD,
CODE_PEMTH.CODE_DESC AS payment_received_method_description,
payments.PAYMENT_SOURCE_CD,
CODE_PAYSRC.CODE_DESC AS payment_source_description,
payments.TRANSACTION_TYPE_CD,
CODE_PAYSRC_TRANSACTION.CODE_DESC AS payment_type_description,
payments.REASON_CD,
payments.PAID_BY_CD,
payments.ADJUSTMENT_DESCRIPTION_CD,
CODE_PAYADJ.CODE_DESC AS payment_adjustment_description,
payments.POST_COMPLETED_FL,
payments.POST_DT,
payments.AUTO_RENEW_FL,
payments.LAST_UPD_DT,
TO_BASE64(MD5(CONCAT(ifnull(COALESCE(membership.membership_adw_key,'-1'),''),'|',
ifnull(COALESCE(bill_summary.membership_billing_summary_adw_key,'-1'),''),'|',
ifnull(COALESCE(renewal.member_auto_renewal_card_adw_key,'-1'),''),'|',
ifnull(COALESCE(branch1.aca_office_adw_key,'-1'),''),'|',
ifnull(COALESCE(users1.employee_adw_key,'-1'),''),'|',
ifnull(payments.BATCH_PAYMENT_KY,''),'|',
ifnull(payments.BATCH_NAME,''),'|',
ifnull(payments.EXP_CT,''),'|',
ifnull(payments.EXP_AT,''),'|',
ifnull(payments.CREATE_DT,''),'|',
ifnull(payments.STATUS,''),'|',
ifnull(payments.STATUS_DT,''),'|',
ifnull(payments.PAYMENT_AT,''),'|',
ifnull(payments.PAYMENT_METHOD_CD,''),'|',
ifnull(CODE_PEMTH.CODE_DESC,''),'|',
ifnull(payments.PAYMENT_SOURCE_CD,''),'|',
ifnull(CODE_PAYSRC.CODE_DESC,''),'|',
ifnull(payments.TRANSACTION_TYPE_CD,''),'|',
ifnull(CODE_PAYSRC_TRANSACTION.CODE_DESC,''),'|',
ifnull(payments.REASON_CD,''),'|',
ifnull(payments.PAID_BY_CD,''),'|',
ifnull(payments.ADJUSTMENT_DESCRIPTION_CD,''),'|',
ifnull(CODE_PAYADJ.CODE_DESC,''),'|',
ifnull(payments.POST_COMPLETED_FL,''),'|',
ifnull(payments.POST_DT,''),'|',
ifnull(payments.AUTO_RENEW_FL,''),'|'
))) as adw_row_hash
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_payments_received_work_source` payments
LEFT JOIN
`{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership
ON payments.MEMBERSHIP_KY=SAFE_CAST(membership.membership_source_system_key AS STRING) AND membership.active_indicator='Y'
LEFT JOIN
`{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` bill_summary
ON payments.BILL_KY=SAFE_CAST(bill_summary.bill_summary_source_key AS STRING) AND bill_summary.active_indicator='Y'
LEFT JOIN
(SELECT
branch.BRANCH_KY,
branch.BRANCH_CD,
office.membership_branch_code,
office.aca_office_adw_key,
ROW_NUMBER() OVER (PARTITION BY branch.BRANCH_KY ORDER BY NULL) AS DUPE_CHECK
FROM
`{iu.INGESTION_PROJECT}.mzp.branch` branch,
`{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` office
where office.membership_branch_code=branch.BRANCH_CD and office.active_indicator='Y'
) as branch1 
on branch1.BRANCH_KY=payments.BRANCH_KY AND branch1.DUPE_CHECK=1
LEFT JOIN
(SELECT users.USER_ID,
users.USERNAME,
emp.employee_active_directory_user_identifier,
emp.employee_adw_key,
ROW_NUMBER() OVER (PARTITION BY users.USER_ID ORDER BY NULL) AS DUPE_CHECK
FROM `{iu.INGESTION_PROJECT}.mzp.cx_iusers` users,
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` emp
where emp.employee_active_directory_user_identifier=users.USERNAME and emp.active_indicator='Y'
) users1 ON users1.USER_ID=payments.USER_ID AND users1.DUPE_CHECK=1
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYADJ'
GROUP BY CODE) CODE_PAYADJ
ON CODE_PAYADJ.CODE=payments.ADJUSTMENT_DESCRIPTION_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PEMTH'
GROUP BY CODE) CODE_PEMTH
ON CODE_PEMTH.CODE=payments.PAYMENT_METHOD_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC
ON CODE_PAYSRC.CODE=payments.PAYMENT_SOURCE_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC_TRANSACTION
ON CODE_PAYSRC_TRANSACTION.CODE=payments.TRANSACTION_TYPE_CD
LEFT OUTER JOIN
`{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` renewal
ON SAFE_CAST(renewal.member_source_arc_key AS STRING)=payments.AUTORENEWAL_CARD_KY AND renewal.active_indicator='Y'
where payments.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_payments_received_work_final_staging = f"""

CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_stage` AS
    SELECT
    COALESCE(target.membership_payments_received_adw_key,
      GENERATE_UUID()) membership_payments_received_adw_key,
      source.membership_adw_key membership_adw_key,
source.membership_billing_summary_adw_key membership_billing_summary_adw_key ,
source.member_auto_renewal_card_adw_key member_auto_renewal_card_adw_key ,
source.aca_office_adw_key aca_office_adw_key ,
source.employee_adw_key employee_adw_key ,
SAFE_CAST(source.BATCH_PAYMENT_KY AS INT64) payment_received_source_key ,
SAFE_CAST(source.BATCH_KY AS INT64) payment_received_batch_key ,
source.BATCH_NAME payment_received_name,
SAFE_CAST(source.EXP_CT AS INT64) payment_received_count ,
SAFE_CAST(source.EXP_AT AS NUMERIC) payment_received_expected_amount ,
SAFE_CAST(source.CREATE_DT AS DATETIME) payment_received_created_datetime ,
source.STATUS payment_received_status ,
SAFE_CAST(source.STATUS_DT AS DATETIME) payment_received_status_datetime ,
SAFE_CAST(source.PAYMENT_AT AS NUMERIC) payment_received_amount ,
source.PAYMENT_METHOD_CD payment_received_method_code ,
source.payment_received_method_description payment_received_method_description ,
source.PAYMENT_SOURCE_CD payment_source_code ,
source.payment_source_description payment_source_description ,
source.TRANSACTION_TYPE_CD payment_type_code ,
source.payment_type_description payment_type_description ,
source.REASON_CD payment_reason_code ,
source.PAID_BY_CD payment_paid_by_description ,
source.ADJUSTMENT_DESCRIPTION_CD payment_adjustment_code ,
source.payment_adjustment_description payment_adjustment_description ,
source.POST_COMPLETED_FL payment_received_post_flag ,
SAFE_CAST(source.POST_DT AS DATETIME) payment_received_post_datetime ,
source.AUTO_RENEW_FL payment_received_auto_renewal_flag ,
      SAFE_CAST(last_upd_dt AS DATETIME) AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` target
  ON
    (source.batch_payment_ky=SAFE_CAST(target.payment_received_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_payments_received_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
    UNION ALL
    SELECT
    target.membership_payments_received_adw_key,
target.membership_adw_key,
target.membership_billing_summary_adw_key,
target.member_auto_renewal_card_adw_key,
target.aca_office_adw_key,
target.employee_adw_key,
target.payment_received_source_key,
target.payment_received_batch_key,
target.payment_received_name,
target.payment_received_count,
target.payment_received_expected_amount,
target.payment_received_created_datetime,
target.payment_received_status,
target.payment_received_status_datetime,
target.payment_received_amount,
target.payment_received_method_code,
target.payment_received_method_description,
target.payment_source_code,
target.payment_source_description,
target.payment_type_code,
target.payment_type_description,
target.payment_reason_code,
target.payment_paid_by_description,
target.payment_adjustment_code,
target.payment_adjustment_description,
target.payment_received_post_flag,
target.payment_received_post_datetime,
target.payment_received_auto_renewal_flag,
target.effective_start_datetime,
     DATETIME_SUB(SAFE_CAST(source.last_upd_dt AS DATETIME),
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` target
  ON
    (source.batch_payment_ky=SAFE_CAST(target.payment_received_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash


"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_payments_received
membership_payments_received_merge = f"""

MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_stage` b
  ON
    (a.membership_payments_received_adw_key = b.membership_payments_received_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT
    (
    membership_payments_received_adw_key,
membership_adw_key,
membership_billing_summary_adw_key,
member_auto_renewal_card_adw_key,
aca_office_adw_key,
employee_adw_key,
payment_received_source_key,
payment_received_batch_key,
payment_received_name,
payment_received_count,
payment_received_expected_amount,
payment_received_created_datetime,
payment_received_status,
payment_received_status_datetime,
payment_received_amount,
payment_received_method_code,
payment_received_method_description,
payment_source_code,
payment_source_description,
payment_type_code,
payment_type_description,
payment_reason_code,
payment_paid_by_description,
payment_adjustment_code,
payment_adjustment_description,
payment_received_post_flag,
payment_received_post_datetime,
payment_received_auto_renewal_flag,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
    ) VALUES
  (  b.membership_payments_received_adw_key,
b.membership_adw_key,
b.membership_billing_summary_adw_key,
b.member_auto_renewal_card_adw_key,
b.aca_office_adw_key,
b.employee_adw_key,
b.payment_received_source_key,
b.payment_received_batch_key,
b.payment_received_name,
b.payment_received_count,
b.payment_received_expected_amount,
b.payment_received_created_datetime,
b.payment_received_status,
b.payment_received_status_datetime,
b.payment_received_amount,
b.payment_received_method_code,
b.payment_received_method_description,
b.payment_source_code,
b.payment_source_description,
b.payment_type_code,
b.payment_type_description,
b.payment_reason_code,
b.payment_paid_by_description,
b.payment_adjustment_code,
b.payment_adjustment_description,
b.payment_received_post_flag,
b.payment_received_post_datetime,
b.payment_received_auto_renewal_flag,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number)
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
"""

# Membership_fee (SAHIL)

######################################################################
# Load membership_fee Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_fee_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_source` AS
SELECT
membership_fee_source.MEMBERSHIP_FEES_KY,
membership_fee_source.membership_ky,
membership_fee_source.status,
membership_fee_source.fee_type,
membership_fee_source.fee_dt,
membership_fee_source.waived_dt,
membership_fee_source.waived_by,
membership_fee_source.donor_nr,
membership_fee_source.waived_reason_cd,
membership_fee_source.last_upd_dt, 
  ROW_NUMBER() OVER(PARTITION BY membership_fee_source.membership_ky  ORDER BY membership_fee_source.last_upd_dt desc) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.membership_fees` AS membership_fee_source
 WHERE
   CAST(membership_fee_source.last_upd_dt AS datetime) > (
   SELECT
    MAX(effective_start_datetime)
   FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee`)

"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_fee_work_transformed = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` AS
SELECT
  COALESCE(dim_member.member_adw_key,
    "-1") AS member_adw_key,
  COALESCE(product1.product_adw_key,
    "-1") AS product_adw_key,
  SAFE_CAST(COALESCE(membership_fee_source.MEMBERSHIP_FEES_KY,
    "-1")as int64) AS membership_fee_source_key,
  membership_fee_source.status AS status,
  membership_fee_source.fee_type AS fee_type,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.fee_dt,1,10)) AS Date) AS fee_date,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_fee_source.waived_dt,1,10)) AS Date) AS waived_date,
  SAFE_CAST(membership_fee_source.waived_by as INT64)  AS waived_by,
  membership_fee_source.donor_nr AS donor_number,
  membership_fee_source.waived_reason_cd AS waived_reason_code,
  membership_fee_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(dim_member.member_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(product1.product_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(membership_fee_source.MEMBERSHIP_FEES_KY,
            "-1"),
          ''), '|', ifnull(membership_fee_source.status,
          ''), '|', ifnull(membership_fee_source.fee_type,
          ''), '|',ifnull(membership_fee_source.fee_dt,
          ''), '|',ifnull(membership_fee_source.waived_dt,
          ''), '|',ifnull(membership_fee_source.waived_by,
          ''), '|',ifnull(membership_fee_source.donor_nr,
          ''), '|',ifnull(membership_fee_source.waived_reason_cd,
          '') ))) AS adw_row_hash
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_source` AS membership_fee_source
LEFT OUTER JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_member` dim_member
ON
  membership_fee_source.MEMBERSHIP_FEES_KY=CAST(dim_member.member_source_system_key AS string)
  AND dim_member.active_indicator='Y'
LEFT OUTER JOIN (
  SELECT
    membership.membership_ky,
    product_adw_key,
    product_sku_key,
    active_indicator,
    ROW_NUMBER() OVER(PARTITION BY membership.membership_ky ORDER BY NULL ) AS dupe_check
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw.dim_product` product,
    `{iu.INGESTION_PROJECT}.mzp.membership` membership
  WHERE
    membership.coverage_level_cd = product.product_sku_key
    AND product.active_indicator='Y' ) product1
ON
  membership_fee_source.membership_ky=product1.membership_ky
  AND product1.dupe_check=1
WHERE
  membership_fee_source.dupe_check=1
"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_fee_work_final_staging = f"""


CREATE OR REPLACE TABLE  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_stage` AS
SELECT 
   COALESCE(target.membership_fee_adw_key,
      GENERATE_UUID()) membership_fee_adw_key,
  source.member_adw_key,
  source.product_adw_key,
  source.membership_fee_source_key  as membership_fee_source_key,
  source.status,
  source.fee_type,
  source.fee_date,
  source.waived_date,
  source.waived_by as waived_by,
  source.donor_number as donor_number,
  source.waived_reason_code as waived_reason_code ,
  SAFE_CAST(source.effective_start_datetime as DATETIME) as effective_start_datetime ,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS active_indicator,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  1 integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  1 integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` target
ON
  (source.membership_fee_source_key=target.membership_fee_source_key
    AND target.active_indicator='Y')
WHERE
  target.membership_fee_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  membership_fee_adw_key,
  target.member_adw_key,
  target.product_adw_key,
  target.membership_fee_source_key,
  target.status,
  target.fee_type,
  target.fee_date,
  target.waived_date,
  target.waived_by,
  target.donor_number,
  target.waived_reason_code ,
  target.effective_start_datetime,
  DATETIME_SUB(cast(source.effective_start_datetime as datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1 
 FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` target
ON
  (source.membership_fee_source_key=target.membership_fee_source_key 
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership_fee
membership_fee_merge = f"""

MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_fee_work_stage` b
ON
  (a.membership_fee_source_key = b.membership_fee_source_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  membership_fee_adw_key, 
  member_adw_key , 
  product_adw_key , 
  membership_fee_source_key , 
  status , 
  fee_type , 
  fee_date , 
  waived_date,
  waived_by,
  donor_number,
  waived_reason_code,
  effective_start_datetime, 
  effective_end_datetime, 
  active_indicator, 
  adw_row_hash, 
  integrate_insert_datetime, 
  integrate_insert_batch_number, 
  integrate_update_datetime, 
  integrate_update_batch_number ) VALUES (
  b.membership_fee_adw_key , 
  b.member_adw_key , 
  b.product_adw_key , 
  b.membership_fee_source_key , 
  b.status , 
  b.fee_type , 
  b.fee_date , 
  b.waived_date,
  b.waived_by,
  b.donor_number,
  b.waived_reason_code,
  b.effective_start_datetime, 
  b.effective_end_datetime, 
  b.active_indicator, 
  b.adw_row_hash, 
  b.integrate_insert_datetime, 
  b.integrate_insert_batch_number, 
  b.integrate_update_datetime, 
  b.integrate_update_batch_number
)
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number
"""

######################################################################
# Load Membership_solicitation Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_solicitation_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_solicitation_work_source` AS
  SELECT
  solicitation_source.solicitation_ky,
  solicitation_source.solicitation_cd,
  solicitation_source.group_cd,
  solicitation_source.membership_type_cd,
  solicitation_source.campaign_cd,
  solicitation_source.solicitation_category_cd,
  solicitation_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY solicitation_source.solicitation_ky ORDER BY solicitation_source.last_upd_dt DESC) AS dupe_check
  FROM `{iu.INGESTION_PROJECT}.mzp.solicitation` solicitation_source
  WHERE
  CAST(solicitation_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation`)
"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_solicitation_work_transformed = f"""
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` AS
SELECT
solicitation.solicitation_ky,
  solicitation.solicitation_cd,
  solicitation.group_cd,
  solicitation.membership_type_cd,
  solicitation.campaign_cd,
  solicitation.solicitation_category_cd,
  discount.discount_cd,
  solicitation.last_upd_dt,
TO_BASE64(MD5(CONCAT(ifnull(solicitation.solicitation_cd,''),'|',
ifnull(solicitation.group_cd,''),'|',
ifnull(solicitation.membership_type_cd,''),'|',
ifnull(solicitation.campaign_cd,''),'|',
ifnull(solicitation.solicitation_category_cd,''),'|',
ifnull(discount.discount_cd,''),'|'
))) as adw_row_hash
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_solicitation_work_source` solicitation
LEFT JOIN
(SELECT 
      solicitation_ky,
      discount_cd,
      ROW_NUMBER() OVER(PARTITION BY solicitation_ky ORDER BY adw_lake_insert_datetime DESC) AS dupe_check
    FROM `{iu.INGESTION_PROJECT}.mzp.solicitation_discount`
) discount
on discount.solicitation_ky=solicitation.solicitation_ky and discount.dupe_check=1
where solicitation.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_solicitation_work_final_staging = f"""

CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_stage` AS
SELECT
COALESCE(target.membership_solicitation_adw_key ,GENERATE_UUID()) AS membership_solicitation_adw_key,
SAFE_CAST(source.solicitation_ky AS INT64) AS membership_solicitation_source_system_key ,
source.solicitation_cd AS solicitation_code ,
  source.group_cd AS solicitation_group_code ,
  source.membership_type_cd AS member_type_code ,
  source.campaign_cd AS solicitation_campaign_code ,
  source.solicitation_category_cd AS solicitation_category_code ,
  source.discount_cd AS solicitation_discount_code ,
SAFE_CAST(source.last_upd_dt AS DATETIME) AS effective_start_datetime,
CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash AS adw_row_hash ,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number 
 FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation` target
  ON
    (source.solicitation_ky =SAFE_CAST(target.membership_solicitation_source_system_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_solicitation_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
    target.membership_solicitation_adw_key,
target.membership_solicitation_source_system_key,
target.solicitation_code,
target.solicitation_group_code,
target.member_type_code,
target.solicitation_campaign_code,
target.solicitation_category_code,
target.solicitation_discount_code,
    target.effective_start_datetime,
    DATETIME_SUB(SAFE_CAST(source.last_upd_dt AS DATETIME),
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation` target
  ON
    (source.solicitation_ky=SAFE_CAST(target.membership_solicitation_source_system_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_solicitation
membership_solicitation_merge = f"""
MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_solicitation_work_stage` b
    ON (a.membership_solicitation_adw_key = b.membership_solicitation_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
    membership_solicitation_adw_key,
membership_solicitation_source_system_key,
solicitation_code,
solicitation_group_code,
member_type_code,
solicitation_campaign_code,
solicitation_category_code,
solicitation_discount_code,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number)
VALUES
(
b.membership_solicitation_adw_key,
b.membership_solicitation_source_system_key,
b.solicitation_code,
b.solicitation_group_code,
b.member_type_code,
b.solicitation_campaign_code,
b.solicitation_category_code,
b.solicitation_discount_code,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number
)
 WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number

"""


######################################################################
# Load Payment Plan Incremental set of Data
######################################################################

paymentplan_source = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_source`  as 
SELECT 
 PLAN_BILLING_KY,
 RIDER_KY,
 source.PAYMENT_PLAN_KY,
 PAYMENT_NUMBER,
 MEMBERSHIP_PAYMENT_KY,
 PAYMENT_STATUS,
 CHARGE_DT,
 CHARGE_CT,
 PAYMENT_AT,
 source.LAST_UPD_DT,
 COST_EFFECTIVE_DT,
 MEMBERSHIP_FEES_KY,
 DONATION_HISTORY_KY,
 MEMBERSHIP_KY,
 pp.PLAN_NAME,
 pp.NUMBER_OF_PAYMENTS,
 pp.PLAN_LENGTH,
 ROW_NUMBER() OVER(PARTITION BY source.plan_billing_ky ORDER BY source.last_upd_dt DESC) AS dupe_check
FROM
`{iu.INGESTION_PROJECT}.mzp.plan_billing` as source
left join `{iu.INGESTION_PROJECT}.mzp.payment_plan` as pp on source.PAYMENT_PLAN_KY = pp.PAYMENT_PLAN_KY
where CAST(source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan`)
"""

paymentplan_transformed = f"""
CREATE or REPLACE table `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` as 
SELECT 
 coalesce(adw_memship.membership_adw_key,'-1') as membership_adw_key,
 coalesce(adw_rider.member_rider_adw_key,'-1') as member_rider_adw_key,
 cast(PLAN_BILLING_KY as int64) as payment_plan_source_key ,
 source.PLAN_NAME as payment_plan_name,
 cast(source.NUMBER_OF_PAYMENTS as int64) as payment_plan_payment_count,
 cast(source.PLAN_LENGTH as int64) as payment_plan_months,
 cast(source.PAYMENT_NUMBER as int64) as paymentnumber,
 source.PAYMENT_STATUS as payment_plan_status,
 cast(cast(source.CHARGE_DT as datetime) as date) as payment_plan_charge_date,
 case when source.DONATION_HISTORY_KY is null then 'N' else 'Y' end as safety_fund_donation_indicator,
 CAST(source.last_upd_dt AS datetime) AS last_upd_dt,
 CAST('9999-12-31' AS datetime) effective_end_datetime,
 'Y' as active_indicator,
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
            ifnull(source.DONATION_HISTORY_KY,'')
            ))) as adw_row_hash,
 CURRENT_DATETIME() as integrate_insert_datetime,
 1 as integrate_insert_batch_number,
 CURRENT_DATETIME() as integrate_update_datetime,
 1 as integrate_update_batch_number
FROM
`{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_source` as source 
 LEFT JOIN `{int_u.INTEGRATION_PROJECT}.member.dim_membership` adw_memship
 on source.membership_ky =SAFE_CAST(adw_memship.membership_source_system_key AS STRING) AND adw_memship.active_indicator='Y'
 LEFT JOIN `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider` adw_rider
 on source.rider_ky =SAFE_CAST(adw_rider.member_rider_source_key AS STRING) AND adw_rider.active_indicator='Y'
WHERE source.dupe_check=1 
"""

paymentplan_stage = f"""
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_stage` AS
SELECT
  GENERATE_UUID() as membership_payment_plan_adw_key, 
  source.membership_adw_key,
  source.member_rider_adw_key,
  source.payment_plan_source_key,
  source.payment_plan_name,
  source.payment_plan_payment_count,
  source.payment_plan_months,
  source.paymentnumber,
  source.payment_plan_status,
  source.payment_plan_charge_date,
  source.safety_fund_donation_indicator,
  last_upd_dt AS effective_start_datetime,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  source.active_indicator,
  source.adw_row_hash,
  source.integrate_insert_datetime,
  source.integrate_insert_batch_number,
  source.integrate_update_datetime,
  source.integrate_update_batch_number
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` source
LEFT JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` target
ON
  (source.payment_plan_source_key=target.payment_plan_source_key AND target.active_indicator='Y')
WHERE
  target.payment_plan_source_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
UNION ALL
SELECT
  distinct
  target.membership_payment_plan_adw_key,
  target.membership_adw_key,
  target.member_rider_adw_key,
  target.payment_plan_source_key,
  target.payment_plan_name,
  target.payment_plan_payment_count,
  target.payment_plan_months,
  target.paymentnumber,
  target.payment_plan_status,
  target.payment_plan_charge_date,
  target.safety_fund_donation_indicator,
  target.effective_start_datetime,
  DATETIME_SUB(source.last_upd_dt, INTERVAL 1 day) AS effective_end_datetime,
  'N' AS active_indicator,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  1
FROM
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_transformed` source
JOIN
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` target
ON
  (source.payment_plan_source_key=target.payment_plan_source_key
    AND target.active_indicator='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash

"""

paymentplan_merge = f"""
MERGE INTO
  `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan` a
USING
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payment_plan_stage` b
ON
  (a.membership_payment_plan_adw_key = b.membership_payment_plan_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  membership_payment_plan_adw_key ,    
  membership_adw_key,
  member_rider_adw_key,
  payment_plan_source_key,
  payment_plan_name,
  payment_plan_payment_count,
  payment_plan_months,
  paymentnumber,
  payment_plan_status,
  payment_plan_charge_date,
  safety_fund_donation_indicator,
  effective_end_datetime,
  active_indicator,
  adw_row_hash,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number

  ) VALUES (
  b.membership_payment_plan_adw_key ,    
  b.membership_adw_key,
  b.member_rider_adw_key,
  b.payment_plan_source_key,
  b.payment_plan_name,    
  b.payment_plan_payment_count,
  b.payment_plan_months,
  b.paymentnumber,
  b.payment_plan_status,
  b.payment_plan_charge_date,
  b.safety_fund_donation_indicator,
  b.effective_end_datetime,
  b.active_indicator,
  b.adw_row_hash,
  b.integrate_insert_datetime,
  b.integrate_insert_batch_number,
  b.integrate_update_datetime,
  b.integrate_update_batch_number
  )
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.active_indicator = b.active_indicator,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number
"""

####################################################################################################################################
# Membership_Payment_Applied_Detail
####################################################################################################################################
######################################################################
# Load Membership Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################

member_pay_app_detail_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.
CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_source` AS
SELECT
	membership_source.member_ky,
	membership_source.membership_fees_ky,
	membership_source.rider_ky,
	membership_source.membership_payment_ky,
	membership_source.payment_detail_ky,
	membership_source.membership_payment_at,
	membership_source.unapplied_used_at,
	membership_source.discount_history_ky,
	membership_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY membership_source.payment_detail_ky ORDER BY membership_source.last_upd_dt DESC) AS dupe_check
FROM
  `{iu.INGESTION_PROJECT}.mzp.payment_detail` AS membership_source
WHERE
  CAST(membership_source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_detail`)

"""

######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

member_pay_app_detail_work_transformed = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_transformed` AS
 SELECT
    COALESCE(member.member_adw_key,'-1') AS member_adw_key,
    COALESCE(sub_query.product_adw_key,'-1') AS product_adw_key,
	COALESCE(summary.membership_payment_applied_summary_adw_key,'-1')	 As membership_payment_applied_summary_adw_key,
	COALESCE(membership.payment_detail_ky,'-1') As payment_applied_detail_source_key,
	summary.payment_applied_date As payment_applied_date,
	summary.payment_method_code As payment_method_code,
	summary.payment_method_description As payment_method_description,
	membership.membership_payment_at As payment_detail_amount,
	membership.unapplied_used_at As payment_applied_unapplied_amount,
	disc_hist.amount As discount_amount,
	disc_hist.counted_fl As payment_applied_discount_counted,
	disc_hist.disc_effective_dt	 As discount_effective_date,
	disc_hist.cost_effective_dt As rider_cost_effective_datetime,
	CAST (membership.last_upd_dt AS datetime) As last_upd_dt,
	TO_BASE64(MD5(CONCAT(ifnull(COALESCE(member.member_adw_key,'-1'),
            ''),'|',ifnull(COALESCE(sub_query.product_adw_key,'-1'),
            ''),'|',ifnull(COALESCE(summary.membership_payment_applied_summary_adw_key,'-1'),
            ''),'|',ifnull(COALESCE(membership.payment_detail_ky,'-1'),
            ''),'|',ifnull(safe_cast(summary.payment_applied_date as string),
            ''),'|',ifnull(summary.payment_method_code,
            ''),'|',ifnull(summary.payment_method_description,
            ''),'|',ifnull(membership.membership_payment_at,
            ''),'|',ifnull(membership.unapplied_used_at,
            ''),'|',ifnull(disc_hist.amount,
            ''),'|',ifnull(disc_hist.counted_fl,
            ''),'|',ifnull(disc_hist.disc_effective_dt,
            ''),'|',ifnull(disc_hist.cost_effective_dt,
            '')))) AS adw_row_hash
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_source` AS membership
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.dim_member` AS member
  ON
    membership.member_ky =  safe_cast(member.member_source_system_key as string)
	AND member.active_indicator = 'Y'

  LEFT JOIN	
	(
		SELECT rider_ky as rider_ky, 
			rider_comp_cd as rider_comp_cd ,
			ROW_NUMBER() OVER(PARTITION BY rider_ky ORDER BY last_upd_dt DESC) AS dupe_check
		FROM  
			`{iu.INGESTION_PROJECT}.mzp.rider`  
	) AS rider
  ON 
	membership.rider_ky= rider.rider_ky
	AND rider.dupe_check=1
  LEFT JOIN
	(
		SELECT 	product.product_adw_key,
				product_sku_key
		FROM 
			`{int_u.INTEGRATION_PROJECT}.adw.dim_product` AS product
		LEFT JOIN 
			`{int_u.INTEGRATION_PROJECT}.adw.dim_product_category` AS product_category
		ON 
			product.product_category_adw_key=product_category.product_category_adw_key
			AND product_category.active_indicator = 'Y'
			AND product.active_indicator = 'Y'
		WHERE product_category_code='RCC'
	) sub_query
  ON 
	rider.rider_comp_cd	= sub_query.product_sku_key
  LEFT JOIN
    (
		SELECT 	payment_applied_date,
				payment_method_code,
				payment_method_description ,
				membership_payment_applied_summary_adw_key,
				payment_applied_source_key
		FROM   
			`{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary` 
		WHERE active_indicator = 'Y'
		GROUP BY 	payment_applied_date,
					payment_method_code,
					payment_method_description ,
					membership_payment_applied_summary_adw_key,
					payment_applied_source_key
	) AS summary
  ON
    membership.membership_payment_ky =  safe_cast(summary.payment_applied_source_key as string)
  LEFT JOIN 
    `{iu.INGESTION_PROJECT}.mzp.discount_history` AS disc_hist
  ON
    membership.discount_history_ky = disc_hist.discount_history_ky 
WHERE 
	membership.dupe_check=1

"""

######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

member_pay_app_detail_work_final_staging = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_stage` AS
  SELECT
    COALESCE(target.membership_payment_applied_detail_adw_key,
      GENERATE_UUID()) membership_payment_applied_detail_adw_key,
    source.member_adw_key AS member_adw_key,
	CAST(source.membership_payment_applied_summary_adw_key AS string) AS membership_payment_applied_summary_adw_key,
	CAST (source.product_adw_key AS string) AS product_adw_key,	  
	CAST(source.payment_applied_detail_source_key AS int64) AS payment_applied_detail_source_key,
	source.payment_applied_date AS payment_applied_date,
	CAST(source.payment_method_code AS string) AS payment_method_code,
	source.payment_method_description AS payment_method_description,
	CAST(source.payment_detail_amount AS numeric) AS payment_detail_amount,
	CAST(source.payment_applied_unapplied_amount AS numeric) AS payment_applied_unapplied_amount,
	CAST(source.discount_amount AS numeric) AS discount_amount,
	CAST(source.payment_applied_discount_counted AS string) AS payment_applied_discount_counted,
	CAST(substr(source.discount_effective_date, 0, 10) AS date) AS discount_effective_date,
	CAST(source.rider_cost_effective_datetime AS datetime) AS rider_cost_effective_datetime,
	last_upd_dt AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash AS adw_row_hash,
    CURRENT_DATETIME() AS integrate_insert_datetime,
    1 AS integrate_insert_batch_number,
    CURRENT_DATETIME() AS integrate_update_datetime,
    1 AS integrate_update_batch_number
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_detail` target
  ON
    (source.payment_applied_detail_source_key=safe_cast(target.payment_applied_detail_source_key as string)
      AND target.active_indicator='Y')
  WHERE
    target.member_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
  UNION ALL
  SELECT
    target.membership_payment_applied_detail_adw_key,
    target.member_adw_key,
    target.membership_payment_applied_summary_adw_key,
    target.product_adw_key,	
    target.payment_applied_detail_source_key,
    target.payment_applied_date,
    target.payment_method_code,
    target.payment_method_description,
    target.payment_detail_amount,
    target.payment_applied_unapplied_amount,
    target.discount_amount,
    source.payment_applied_discount_counted,
    target.discount_effective_date,
    target.rider_cost_effective_datetime,
    target.effective_start_datetime,
    DATETIME_SUB(source.last_upd_dt,
      INTERVAL 1 day) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME() AS integrate_update_datetime,
    1 AS integrate_update_batch_number
  FROM
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_detail` target
  ON
    (source.payment_applied_detail_source_key=safe_cast(target.payment_applied_detail_source_key as string)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

"""

#################################
# Load Data warehouse tables from prepared data
#################################

# Membership_payment_applied_detail

membership_pay_app_detail_merge = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

  MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_detail` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_member_pay_app_detail_work_stage` b
  ON
    (a.membership_payment_applied_detail_adw_key = b.membership_payment_applied_detail_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT ( 
	membership_payment_applied_detail_adw_key,
    member_adw_key,
    membership_payment_applied_summary_adw_key,
    product_adw_key,	
    payment_applied_detail_source_key,
    payment_applied_date,
    payment_method_code,
    payment_method_description,
    payment_detail_amount,
    payment_applied_unapplied_amount,
    discount_amount,
    payment_applied_discount_counted,
    discount_effective_date,
    rider_cost_effective_datetime, 
    effective_start_datetime, 
    effective_end_datetime, 
    active_indicator, 
    adw_row_hash, 
    integrate_insert_datetime, 
    integrate_insert_batch_number, 
    integrate_update_datetime, 
    integrate_update_batch_number ) VALUES (
    membership_payment_applied_detail_adw_key,
    member_adw_key,
    membership_payment_applied_summary_adw_key,
    product_adw_key,	
    payment_applied_detail_source_key,
    payment_applied_date,
    payment_method_code,
    payment_method_description,
    payment_detail_amount,
    payment_applied_unapplied_amount,
    discount_amount,
    payment_applied_discount_counted,
    discount_effective_date,
    rider_cost_effective_datetime,
	b.effective_start_datetime,
	b.effective_end_datetime,
    b.active_indicator,
    b.adw_row_hash,
    b.integrate_insert_datetime,
    b.integrate_insert_batch_number,
    b.integrate_update_datetime,
    b.integrate_update_batch_number 
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
"""


with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS
    """
    # Product
    task_product_source = int_u.run_query('product_source', product_source)
    task_product_transformed = int_u.run_query('product_transformed', product_transformed)
    task_product_stage = int_u.run_query('product_stage', product_stage)
    task_product_merge = int_u.run_query('product_merge', product_merge)

    # Membership

    # Membership - Workarea loads
    task_membership_work_source = int_u.run_query("build_membership_work_source", membership_work_source)
    task_membership_work_transformed = int_u.run_query("build_membership_work_transformed", membership_work_transformed)
    task_membership_work_final_staging = int_u.run_query("build_membership_work_final_staging", membership_work_final_staging)

    # membership - Merge Load
    task_membership_merge = int_u.run_query('merge_membership', membership_merge)

    # Member

    # Member - Workarea loads
    task_member_work_source = int_u.run_query("build_member_work_source", member_work_source)
    task_member_work_transformed = int_u.run_query("build_member_work_transformed", member_work_transformed)
    task_member_work_final_staging = int_u.run_query("build_member_work_final_staging", member_work_final_staging)

    # member - Merge Load
    task_member_merge = int_u.run_query('merge_member', member_merge)

    # Member_rider

    # Member_rider - Workarea loads
    task_member_rider_work_source = int_u.run_query("build_member_rider_work_source", member_rider_work_source)
    task_member_rider_work_transformed = int_u.run_query("build_member_rider_work_transformed", member_rider_work_transformed)
    task_member_rider_work_final_staging = int_u.run_query("build_member_rider_work_final_staging", member_rider_work_final_staging)

    # member_rider - Merge Load
    task_member_rider_merge = int_u.run_query('merge_member_rider', member_rider_merge)

    # Employee

    # Employee -Workarea Loads
    task_employee_work_source = int_u.run_query("build_employee_work_source", employee_work_source)
    task_employee_work_transformed = int_u.run_query("build_employee_work_transformed", employee_work_transformed)
    task_employee_work_final_staging = int_u.run_query("build_employee_work_final_staging", employee_work_final_staging)
    # Employee - Merge Load
    task_employee_merge = int_u.run_query('merge_employee', employee_merge)

    # Employee_role
    # Employee_role - Workarea loads
    task_employee_role_work_source = int_u.run_query("build_employee_role_work_source", employee_role_work_source)
    task_employee_role_work_transformed = int_u.run_query("build_employee_role_work_transformed", employee_role_work_transformed)
    task_employee_role_work_final_staging = int_u.run_query("build_employee_role_work_final_staging", employee_role_work_final_staging)

    # member - Merge Load
    task_employee_role_merge = int_u.run_query('merge_employee_role', employee_role_merge)

    # member_comments
    task_membership_comments_source = int_u.run_query('membership_comment_source', membership_comment_source)
    task_membership_comment_fkey = int_u.run_query('membership_comment_fkey', membership_comment_fkey)
    task_membership_comment_stage = int_u.run_query('membership_comment_stage', membership_comment_stage)
    task_membership_comment = int_u.run_query('membership_comment', membership_comment)

    # membership segment
    task_membership_segment_source = int_u.run_query('membership_segment_source', membership_segment_source)
    task_membership_segment_transformed = int_u.run_query('membership_segment_transformed', membership_segment_transformed)
    task_membership_segment_stage = int_u.run_query('membership_segment_stage', membership_segment_stage)
    task_membership_segment_merge = int_u.run_query('membership_segment_merge', membership_segment_merge)

    # member_auto_renewal_card_
    task_member_auto_renewal_card_work_source = int_u.run_query('member_auto_renewal_card_work_source', member_auto_renewal_card_work_source)
    task_member_auto_renewal_card_work_transformed = int_u.run_query('member_auto_renewal_card_work_transformed', member_auto_renewal_card_work_transformed)
    task_member_auto_renewal_card_final_stage = int_u.run_query('member_auto_renewal_card_final_stage', member_auto_renewal_card_final_stage)
    task_merge_member_auto_renewal_card = int_u.run_query('merge_member_auto_renewal_card', merge_member_auto_renewal_card)

    # billing_summary

    # billing_summary - Workarea loads
    task_billing_summary_work_source = int_u.run_query("build_billing_summary_work_source", membership_billing_summary_work_source)
    task_billing_summary_work_transformed = int_u.run_query("build_billing_summary_work_transformed", membership_billing_summary_work_transformed)
    task_billing_summary_work_final_staging = int_u.run_query("build_billing_summary_work_final_staging", membership_billing_summary_work_final_staging)

    # billing_summary - Merge Load
    task_billing_summary_merge = int_u.run_query("merge_billing_summary", membership_billing_summary_merge)

    # billing_detail

    # billing_detail - Workarea loads
    task_billing_detail_work_source = int_u.run_query("build_billing_detail_work_source", membership_billing_detail_work_source)
    task_billing_detail_work_transformed = int_u.run_query("build_billing_detail_work_transformed", membership_billing_detail_work_transformed)
    task_billing_detail_work_final_staging = int_u.run_query("build_billing_detail_work_final_staging", membership_billing_detail_work_final_staging)

    # billing_detail - Merge Load
    task_billing_detail_merge = int_u.run_query("merge_billing_detail", membership_billing_detail_merge)

    # membership_office

    # membership_office - Workarea loads (Prerna)
    task_membership_office_work_source = int_u.run_query("build_membership_office_work_source", membership_office_work_source)
    task_membership_office_work_transformed = int_u.run_query("build_membership_office_work_transformed", membership_office_work_transformed)
    task_membership_office_work_final_staging = int_u.run_query("build_membership_office_work_final_staging", membership_office_work_final_staging)

    # membership_office - Merge Load
    task_membership_office_merge = int_u.run_query('merge_membership_office', membership_office_merge)

    #Payment Summary
    task_payment_applied_summary_source = int_u.run_query("payment_applied_summary_source", payment_applied_summary_source)
    task_payment_applied_summary_transformed = int_u.run_query("payment_applied_summary_transformed", payment_applied_summary_transformed)
    task_payment_summary_stage = int_u.run_query("payment_summary_stage",payment_summary_stage)
    task_payment_summary_merge = int_u.run_query('payment_summary_merge', payment_summary_merge)

    # membership_gl_payments_applied (SAHIL)

    task_membership_gl_payments_applied_work_source = int_u.run_query("membership_gl_payments_applied_work_source", membership_gl_payments_applied_work_source)
    task_membership_gl_payments_applied_work_transformed = int_u.run_query("membership_gl_payments_applied_work_transformed", membership_gl_payments_applied_work_transformed)
    task_membership_gl_payments_applied_work_final_staging = int_u.run_query("membership_gl_payments_applied_work_final_staging", membership_gl_payments_applied_work_final_staging)
    task_membership_gl_payments_applied_merge = int_u.run_query("membership_gl_payments_applied_merge", membership_gl_payments_applied_merge)

    # member_payments_received - Merge Load (Prerna)

    task_membership_payments_received_work_source = int_u.run_query("build_membership_payments_received_work_source", membership_payments_received_work_source)
    task_membership_payments_received_work_transformed = int_u.run_query("build_membership_payments_received_work_transformed", membership_payments_received_work_transformed)
    task_membership_payments_received_work_final_staging = int_u.run_query("build_membership_payments_received_work_final_staging", membership_payments_received_work_final_staging)

    # membership_payments_received - Merge Load
    task_membership_payments_received_merge = int_u.run_query('merge_membership_payments_received', membership_payments_received_merge)

    # Membership_fee(SAHIL)
    # Membership_fee - Workarea loads
    task_membership_fee_work_source = int_u.run_query("build_membership_fee_work_source", membership_fee_work_source)
    task_membership_fee_work_transformed = int_u.run_query("build_membership_fee_work_transformed", membership_fee_work_transformed)
    task_membership_fee_work_final_staging = int_u.run_query("build_membership_fee_work_final_staging", membership_fee_work_final_staging)
    # Membership_fee - Merge Load

    task_membership_fee_merge = int_u.run_query('merge_membership_fee', membership_fee_merge)

    # membership_solicitation - Workarea loads
    task_membership_solicitation_work_source = int_u.run_query("build_membership_solicitation_work_source", membership_solicitation_work_source)
    task_membership_solicitation_work_transformed = int_u.run_query("build_membership_solicitation_work_transformed", membership_solicitation_work_transformed)
    task_membership_solicitation_work_final_staging = int_u.run_query("build_membership_solicitation_work_final_staging", membership_solicitation_work_final_staging)

    # membership_solicitation - Merge Load
    task_membership_solicitation_merge = int_u.run_query('merge_membership_solicitation', membership_solicitation_merge)

    # Payment Plan
    task_paymentplan_source = int_u.run_query('paymentplan_source', paymentplan_source)
    task_paymentplan_transformed = int_u.run_query('paymentplan_transformed', paymentplan_transformed)
    task_paymentplan_stage = int_u.run_query('paymentplan_stage', paymentplan_stage)
    task_paymentplan_merge = int_u.run_query('paymentplan_merge', paymentplan_merge)
    """
    # Membership_Payment_Applied_Detail
    task_member_pay_app_detail_work_source = int_u.run_query('build_member_pay_app_detail_work_source', member_pay_app_detail_work_source)
    task_member_pay_app_detail_work_transformed = int_u.run_query('build_member_pay_app_detail_work_transformed', member_pay_app_detail_work_transformed)
    task_member_pay_app_detail_work_final_staging = int_u.run_query('build_member_pay_app_detail_work_final_staging', member_pay_app_detail_work_final_staging)
    task_membership_pay_app_detail_merge = int_u.run_query('merge_membership_pay_app_detail_merge', membership_pay_app_detail_merge)

    ######################################################################
    # DEPENDENCIES
    # Membership_fee
"""
task_membership_solicitation_work_source >> task_membership_solicitation_work_transformed >> task_membership_solicitation_work_final_staging >> task_membership_solicitation_merge >> task_membership_work_source

    # member auto renewal card
task_member_auto_renewal_card_work_source >> task_member_auto_renewal_card_work_transformed >> task_member_auto_renewal_card_final_stage >> task_merge_member_auto_renewal_card >> task_membership_work_transformed

# Product
task_product_source >> task_product_transformed
task_product_transformed >> task_product_stage
task_product_stage >> task_product_merge

task_product_merge >> task_membership_work_source

task_employee_work_source >> task_employee_work_transformed >> task_employee_work_final_staging >> task_employee_merge >> task_employee_role_work_source >> task_employee_role_work_transformed >> task_employee_role_work_final_staging >> task_employee_role_merge >> task_membership_work_source

# Store
task_membership_office_work_source >> task_membership_office_work_transformed >> task_membership_office_work_final_staging >> task_membership_office_merge >> task_membership_work_source

# Membership

task_membership_work_source >> task_membership_work_transformed
task_membership_work_transformed >> task_membership_work_final_staging
task_membership_work_final_staging >> task_membership_merge
# Member
task_membership_merge >> task_member_work_source

task_member_work_source >> task_member_work_transformed
task_member_work_transformed >> task_member_work_final_staging
task_member_work_final_staging >> task_member_merge
# Member_rider
task_member_merge >> task_member_rider_work_source

task_member_rider_work_source >> task_member_rider_work_transformed
task_member_rider_work_transformed >> task_member_rider_work_final_staging

task_member_rider_work_final_staging >> task_member_rider_merge

task_membership_merge >> task_membership_comments_source >> task_membership_comment_fkey >> task_membership_comment_stage >> task_membership_comment

# Membership segment
task_membership_merge >> task_membership_segment_source >> task_membership_segment_transformed >> task_membership_segment_stage >> task_membership_segment_merge

# billing_summary
task_membership_merge >> task_billing_summary_work_source >> task_billing_summary_work_transformed >> task_billing_summary_work_final_staging >> task_billing_summary_merge >> task_billing_detail_work_source >> task_billing_detail_work_transformed >> task_billing_detail_work_final_staging >> task_billing_detail_merge

#payment_summary
task_membership_merge >> task_payment_applied_summary_source >> task_payment_applied_summary_transformed >> task_payment_summary_stage >> task_payment_summary_merge

# membership_gl_payments_applied
task_membership_merge >> task_membership_gl_payments_applied_work_source >> task_membership_gl_payments_applied_work_transformed >> task_membership_gl_payments_applied_work_final_staging >> task_membership_gl_payments_applied_merge

# membership_payments received
task_membership_merge >> task_membership_payments_received_work_source >> task_membership_payments_received_work_transformed >> task_membership_payments_received_work_final_staging >> task_membership_payments_received_merge

# Membership_fee
task_member_merge >> task_membership_fee_work_source >> task_membership_fee_work_transformed >> task_membership_fee_work_final_staging >> task_membership_fee_merge

# Payment Plan
task_member_rider_merge >> task_paymentplan_source >> task_paymentplan_transformed >> task_paymentplan_stage >> task_paymentplan_merge
"""


# membership_payment_applied_detail
task_member_pay_app_detail_work_source >> task_member_pay_app_detail_work_transformed >> task_member_pay_app_detail_work_final_staging >> task_membership_pay_app_detail_merge

