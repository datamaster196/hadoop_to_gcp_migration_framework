from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_mzp_membership_office"

default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'start_date'      : datetime(2019, 8, 22),
    'email'           : ['airflow@example.com'],
    'catchup'         : False,
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=5),
}



######################################################################
# Load Membership_office Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


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
    MAX(effective_start_datetime)
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
source.Code as aca_office_list_source_key,
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
  (source.code=target.aca_office_list_source_key
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
  (source.code=target.aca_office_list_source_key
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

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    # membership_office

    # membership_office - Workarea loads
    task_membership_office_work_source = int_u.run_query("build_membership_office_work_source", membership_office_work_source)
    task_membership_office_work_transformed = int_u.run_query("build_membership_office_work_transformed", membership_office_work_transformed)
    task_membership_office_work_final_staging = int_u.run_query("build_membership_office_work_final_staging",
                                                         membership_office_work_final_staging)

    # membership_office - Merge Load
    task_membership_office_merge = int_u.run_query('merge_membership_office', membership_office_merge)


    ######################################################################
    # DEPENDENCIES



    # membership_office

    task_membership_office_work_source >> task_membership_office_work_transformed
    task_membership_office_work_transformed >> task_membership_office_work_final_staging

    task_membership_office_work_final_staging >> task_membership_office_merge
    

