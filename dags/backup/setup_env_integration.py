from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u


SOURCE_DB_NAMES = ['epic', 'd3', 'mzp']
DAG_TITLE = 'setup_env_integrate'
INGESTION_PROJECT = '{{ var.value.INGESTION_PROJECT }}'

def bq_make_dataset(source_db, project=f'{int_u.INTEGRATION_PROJECT}'):
    task_id = f'create-dataset-{iu.ENV}-{source_db}'
    cmd = f"""bq ls "{project}:{source_db}" || bq mk "{project}:{source_db}" """

    return BashOperator(task_id=task_id, bash_command=cmd)

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
    'file_partition': iu.build_file_partition(),
    'params': {
        'iproject': int_u.INTEGRATION_PROJECT,
        'integration_project': int_u.INTEGRATION_PROJECT,
        'ingestion_project': iu.INGESTION_PROJECT
              }
}

#def bq_make_dataset(source_db, project=f'{int_u.INTEGRATION_PROJECT}'):
#    task_id = f'create-dataset-{iu.ENV}-{source_db}'
#    cmd = f"""bq ls "{project}:{source_db}" || bq mk "{project}:{source_db}" """
#
#    return BashOperator(task_id=task_id, bash_command=cmd)



def bq_adw_pii_dim_address():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_address`(
    address_adw_key                   STRING         NOT NULL,
    raw_address_1_name                STRING,
    raw_address_2_name                STRING,
    raw_care_of_name                  STRING,
    raw_city_name                     STRING,
    raw_state_code                    STRING,
    raw_postal_code                   STRING,
    raw_postal_plus_4_code            STRING,
    raw_country_name                  STRING,
    cleansed_address_1_name           STRING,
    cleansed_address_2_name           STRING,
    cleansed_care_of_name             STRING,
    cleansed_city_name                STRING,
    cleansed_state_code               STRING,
    cleansed_state_name               STRING,
    cleansed_postal_code              STRING,
    cleansed_postal_plus_4_code       STRING,
    cleansed_country_name             STRING,
    latitude_number                   NUMERIC,
    longitude_number                  NUMERIC,
    county_name                       STRING,
    county_fips                       STRING,
    time_zone_name                    STRING,
    address_type_code                 STRING,
    address_validation_status_code    STRING,
    address_validation_type           STRING,
    integrate_insert_datetime         DATETIME             NOT NULL,
    integrate_insert_batch_number     INT64          NOT NULL,
    integrate_update_datetime         DATETIME             NOT NULL,
    integrate_update_batch_number     INT64          NOT NULL
)
;"""

    task = int_u.run_query('adw_pii_build_dim_address', sql)
    return task


def bq_adw_pii_dim_contact_info():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_contact_info`(
    contact_adw_key                   STRING        NOT NULL,
    contact_type_code                 STRING,
    contact_first_name                STRING,
    contact_middle_name               STRING,
    contact_last_name                 STRING,
    contact_suffix_name               STRING,
    contact_title_name                STRING,
    contact_address_1_name            STRING,
    contact_address_2_name            STRING,
    contact_care_of_name              STRING,
    contact_city_name                 STRING,
    contact_state_code                STRING,
    contact_state_name                STRING,
    contact_postal_code               STRING,
    contact_postal_plus_4_code        STRING,
    contact_country_name              STRING,
    contact_phone_number              STRING,
    contact_phone_formatted_number    STRING,
    contact_phone_extension_number    INT64,
    contact_email_name                STRING,
    contact_gender_code               STRING,
    contact_gender_name               STRING,
    contact_birth_date                DATE,
    adw_row_hash                      STRING        NOT NULL,
    integrate_insert_datetime         DATETIME            NOT NULL,
    integrate_insert_batch_number     INT64         NOT NULL,
    integrate_update_datetime         DATETIME            NOT NULL,
    integrate_update_batch_number     INT64         NOT NULL
)
;"""

    task = int_u.run_query('adw_pii_build_dim_contact_info', sql)
    return task

def bq_adw_pii_dim_email():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_email`(
    email_adw_key                    STRING        NOT NULL,
    raw_email_name                   STRING,
    cleansed_email_name              STRING,
    email_top_level_domain_name      STRING,
    email_domain_name                STRING,
    email_validation_status_code     STRING,
    email_validation_type            STRING,
    integrate_insert_datetime        DATETIME            NOT NULL,
    integrate_insert_batch_number    INT64         NOT NULL,
    integrate_update_datetime        DATETIME            NOT NULL,
    integrate_update_batch_number    INT64         NOT NULL
)
;"""

    task = int_u.run_query('adw_pii_build_dim_email', sql)
    return task

def bq_adw_pii_dim_employee():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee`(
    employee_adw_key                             STRING     NOT NULL,
    active_directory_email_adw_key               STRING        NOT NULL,
    email_adw_key                                STRING        NOT NULL,
    phone_adw_key                                STRING        NOT NULL,
    fax_phone_adw_key                            STRING        NOT NULL,
    name_adw_key                                 STRING        NOT NULL,
    employee_type_code                           STRING,
    employee_active_directory_user_identifier    STRING,
    employee_hr_identifier                       STRING,
    employee_hr_hire_date                        DATE,
    employee_hr_termination_date                 DATE,
    employee_hr_termination_reason               STRING,
    employee_hr_status                           STRING,
    employee_hr_region                           STRING,
    employee_hr_location                         STRING,
    employee_hr_title                            STRING,
    employee_hr_supervisor                       STRING,
    employee_hr_job_code                         STRING,
    employee_hr_cost_center                      STRING,
    employee_hr_position_type                    STRING,
    effective_start_datetime                     DATETIME            NOT NULL,
    effective_end_datetime                       DATETIME            NOT NULL,
    active_indicator                             STRING         NOT NULL,
    adw_row_hash                                 STRING,
    integrate_insert_datetime                    DATETIME            NOT NULL,
    integrate_insert_batch_number                INT64         NOT NULL,
    integrate_update_datetime                    DATETIME            NOT NULL,
    integrate_update_batch_number                INT64         NOT NULL
)
;"""

    task = int_u.run_query('adw_pii_build_dim_employee', sql)
    return task

def bq_adw_pii_dim_name():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_name`(
    name_adw_key                     STRING        NOT NULL,
    raw_first_name                   STRING,
    raw_middle_name                  STRING,
    raw_last_name                    STRING,
    raw_suffix_name                  STRING,
    raw_title_name                   STRING,
    cleansed_first_name              STRING,
    cleansed_middle_name             STRING,
    cleansed_last_name               STRING,
    cleansed_suffix_name             STRING,
    cleansed_title_name              STRING,
    name_validation_status_code      STRING,
    name_validation_type             STRING,
    integrate_insert_datetime        DATETIME            NOT NULL,
    integrate_insert_batch_number    INT64         NOT NULL,
    integrate_update_datetime        DATETIME            NOT NULL,
    integrate_update_batch_number    INT64         NOT NULL
)
;"""

    task = int_u.run_query('adw_pii_build_dim_name', sql)
    return task

def bq_adw_pii_dim_phone():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw_pii.dim_phone`(
    phone_adw_key                      STRING       NOT NULL,
    raw_phone_number                   STRING,
    cleansed_phone_number              STRING,
    cleansed_phone_area_code_number    INT64,
    cleansed_phone_prefix_number       INT64,
    cleansed_phone_suffix_number       INT64,
    cleansed_phone_extension_number    INT64,
    phone_validation_status_code       STRING,
    phone_validation_type              STRING,
    integrate_insert_datetime          DATETIME           NOT NULL,
    integrate_insert_batch_number      INT64        NOT NULL,
    integrate_update_datetime          DATETIME           NOT NULL,
    integrate_update_batch_number      INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_pii_build_dim_phone', sql)
    return task
	
def bq_adw_audit_contact_match():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.audit_contact_match` (
    source_system_name string NOT NULL,
    source_system_key_1 string,
    source_system_key_2 string,
    source_system_key_3 string,
    contact_adw_key string NOT NULL,
    target_match_on_source_key_indicator string,
    target_match_on_member_id_indicator string,
    target_match_on_name_address_indicator string,
    target_match_on_name_zip_indicator string,
    target_match_on_name_email_indicator string,
    source_match_on_member_id_indicator string,
    source_match_on_name_address_indicator string,
    source_match_on_name_zip_indicator string,
    source_match_on_name_email_indicator string,
    integrate_insert_datetime datetime,
    integrate_insert_batch_number int64
)
;
"""

    task = int_u.run_query('bq_adw_audit_contact_match', sql)
    return task	

def bq_adw_share_club():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_club`(
    club_adw_key                     STRING    NOT NULL,
    address_adw_key                  STRING,
    phone_adw_key                    STRING,
    club_code                        STRING,
    club_name                        STRING,
    club_iso_code                    STRING,
    club_merchant_number             STRING,
    effective_start_datetime         DATETIME,
    effective_end_datetime           DATETIME,
    active_indicator                 STRING,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64
)
;
"""

    task = int_u.run_query('adw_share_build_club', sql)
    return task

def bq_adw_dim_aca_office():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office`(
    aca_office_adw_key                         STRING     NOT NULL,
    aca_office_address                         STRING,
    aca__office_state_code                     STRING,
    aca_office_phone_number                    STRING,
    aca_office_list_source_key                 STRING     NOT NULL,
    aca_office_name                            STRING,
    aca_office_status                          STRING,
    aca_office_type                            STRING,
    membership_branch_code                     STRING,
    insurance_department_code                  STRING,
    roadside_assistance_communicator_center    STRING,
    car_care_location_number                   STRING,
    travel_branch_code                         STRING,
    aca_office_division                        STRING,
    retail_sales_office_code                   STRING,
    retail_manager                             STRING,
    retail_manager_email                       STRING,
    retail_assistant_manager                   STRING,
    retail_assistant_manager_email             STRING,
    staff_assistant                            STRING,
    staff_assistant_email                      STRING,
    car_care_manager                           STRING,
    car_care_manager_email                     STRING,
    car_care_assistant_manager                 STRING,
    car_care_assistant_manager_email           STRING,
    regional_travel_sales_manager              STRING,
    regional_travel_sales_manager_email        STRING,
    retail_sales_coach                         STRING,
    retail_sales_coach_email                   STRING,
    xactly_retail_store_number                 STRING,
    xactly_car_care_store_number               STRING,
    region                                     STRING,
    car_care_general_manager                   STRING,
    car_care_general_manager_email             STRING,
    car_care_director                          STRING,
    car_care_director_email                    STRING,
    car_care_district_number                   STRING,
    car_care_cost_center_number                STRING,
    tire_cost_center_number                    STRING,
    admin_cost_center_number                   STRING,
    occupancy_cost_center_number               STRING,
    retail_general_manager                     STRING,
    retail_general_manager_email               STRING,
    district_manager                           STRING,
    district_manager_email                     STRING,
    retail_district_number                     STRING,
    retail_cost_center_number                  STRING,
    travel_cost_center_number                  STRING,
    membership_cost_center_number              STRING,
    facility_cost_center_number                STRING,
    national_ats_number                        STRING,
    effective_start_datetime                   DATETIME            NOT NULL,
    effective_end_datetime                     DATETIME            NOT NULL,
    active_indicator                           STRING         NOT NULL,
    adw_row_hash                               STRING,
    integrate_insert_datetime                  DATETIME            NOT NULL,
    integrate_insert_batch_number              INT64         NOT NULL,
    integrate_update_datetime                  DATETIME            NOT NULL,
    integrate_update_batch_number              INT64         NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_aca_office', sql)
    return task

def bq_adw_dim_business_line():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_business_line`(
    business_line_adw_key            STRING    NOT NULL,
    business_line_code               STRING    NOT NULL,
    business_line_description        STRING,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_business_line', sql)
    return task

def bq_adw_dim_comment():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_comment`(
    comment_adw_key                  STRING      NOT NULL,
    created_employee_adw_key         STRING      NOT NULL,
    resolved_employee_adw_key        STRING      NOT NULL,
    comment_relation_adw_key         STRING      NOT NULL,
    comment_source_system_name       STRING,
    comment_source_system_key        INT64,
    comment_creation_datetime        DATETIME,
    comment_text                     STRING,
    comment_resolved_datetime        DATETIME,
    comment_resolution_text          STRING,
    effective_start_datetime         DATETIME             NOT NULL,
    effective_end_datetime           DATETIME             NOT NULL,
    active_indicator                 STRING          NOT NULL,
    adw_row_hash                     STRING,
    integrate_insert_datetime        DATETIME             NOT NULL,
    integrate_insert_batch_number    INT64          NOT NULL,
    integrate_update_datetime        DATETIME             NOT NULL,
    integrate_update_batch_number    INT64          NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_comment', sql)
    return task

def bq_adw_dim_contact_source_key():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_contact_source_key`(
    contact_adw_key                  STRING        NOT NULL,
    contact_source_system_name       STRING     NOT NULL,
    key_type_name                    STRING     NOT NULL,
    effective_start_datetime         DATETIME            NOT NULL,
    source_1_key                     STRING,
    source_2_key                     STRING,
    source_3_key                     STRING,
    source_4_key                     STRING,
    source_5_key                     STRING,
    effective_end_datetime           DATETIME            NOT NULL,
    active_indicator                 STRING         NOT NULL,
    adw_row_hash                     STRING        NOT NULL,
    integrate_insert_datetime        DATETIME            NOT NULL,
    integrate_insert_batch_number    INT64         NOT NULL,
    integrate_update_datetime        DATETIME            NOT NULL,
    integrate_update_batch_number    INT64         NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_contact_source_key', sql)
    return task

def bq_adw_dim_employee_role():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_employee_role`(
    employee_role_adw_key                STRING    NOT NULL,
    employee_adw_key                     STRING    NOT NULL,
    name_adw_key                         STRING       NOT NULL,
    aca_office_adw_key                   STRING    NOT NULL,
    employee_role__business_line_code    STRING,
    employee_role_id                     STRING,
    employee_role_type                   STRING,
    effective_start_datetime             DATETIME           NOT NULL,
    effective_end_datetime               DATETIME           NOT NULL,
    active_indicator                     STRING        NOT NULL,
    adw_row_hash                         STRING,
    integrate_insert_datetime            DATETIME           NOT NULL,
    integrate_insert_batch_number        INT64        NOT NULL,
    integrate_update_datetime            DATETIME           NOT NULL,
    integrate_update_batch_number        INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_employee_role', sql)
    return task

def bq_adw_dim_product():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_product`(
    product_adw_key                    STRING       NOT NULL,
    product_category_adw_key           STRING       NOT NULL,
    vendor_adw_key                     STRING       NOT NULL,
    product_sku                        STRING       NOT NULL,
    product_sku_key                    STRING,
    product_sku_description            STRING,
    product_sku_effective_date         DATE,
    product_sku_expiration_date        DATE,
    product_item_number_code           STRING,
    product_item_description           STRING,
    product_vendor_name                STRING,
    product_service_category_code      STRING,
    product_status_code                STRING,
    product_dynamic_sku                STRING,
    product_dynamic_sku_description    STRING,
    product_dynamic_sku_key            STRING,
    product_unit_cost                  NUMERIC,
    effective_start_datetime           DATETIME              NOT NULL,
    effective_end_datetime             DATETIME              NOT NULL,
    active_indicator                   STRING           NOT NULL,
    adw_row_hash                       STRING          NOT NULL,
    integrate_insert_datetime          DATETIME              NOT NULL,
    integrate_insert_batch_number      INT64           NOT NULL,
    integrate_update_datetime          DATETIME              NOT NULL,
    integrate_update_batch_number      INT64           NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_product', sql)
    return task

def bq_adw_dim_product_category():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_product_category`(
    product_category_adw_key         STRING    NOT NULL,
    business_line_adw_key            STRING    NOT NULL,
    product_category_code            STRING    NOT NULL,
    product_category_description     STRING,
    product_category_status_code     STRING,
    effective_start_datetime         DATETIME           NOT NULL,
    effective_end_datetime           DATETIME           NOT NULL,
    active_indicator                 STRING        NOT NULL,
    adw_row_hash                     STRING       NOT NULL,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_product_category', sql)
    return task

def bq_adw_dim_vendor():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.dim_vendor`(
    vendor_adw_key                   STRING     NOT NULL,
    business_line_adw_key            STRING     NOT NULL,
    address_adw_key                  STRING        NOT NULL,
    email_adw_key                    STRING        NOT NULL,
    vendor_code                      STRING,
    vendor_name                      STRING,
    vendor_type                      STRING,
    internal_facility_code           INT64,
    vendor_contact                   STRING,
    preferred_vendor                 STRING,
    vendor_status                    STRING,
    service_region                   STRING,
    service_region_description       STRING,
    fleet_indicator                  STRING,
    aaa_facility                     STRING,
    effective_start_datetime         DATETIME            NOT NULL,
    effective_end_datetime           DATETIME            NOT NULL,
    active_indicator                 STRING         NOT NULL,
    adw_row_hash                     STRING        NOT NULL,
    integrate_insert_datetime        DATETIME            NOT NULL,
    integrate_insert_batch_number    INT64         NOT NULL,
    integrate_update_datetime        DATETIME            NOT NULL,
    integrate_update_batch_number    INT64         NOT NULL
)
;
"""

    task = int_u.run_query('adw_dim_vendor', sql)
    return task

def bq_adw_xref_contact_address():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_address`(
    contact_adw_key                  STRING       NOT NULL,
    contact_source_system_name       STRING    NOT NULL,
    address_type_code                STRING    NOT NULL,
    effective_start_datetime         DATETIME           NOT NULL,
    address_adw_key                  STRING       NOT NULL,
    effective_end_datetime           DATETIME,
    active_indicator                 STRING        NOT NULL,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_xref_contact_address', sql)
    return task

def bq_adw_xref_contact_email():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_email`(
    contact_adw_key                  STRING       NOT NULL,
    contact_source_system_name       STRING    NOT NULL,
    email_type_code                  STRING    NOT NULL,
    effective_start_datetime         DATETIME           NOT NULL,
    email_adw_key                    STRING       NOT NULL,
    effective_end_datetime           DATETIME,
    active_indicator                 STRING        NOT NULL,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_xref_contact_email', sql)
    return task

def bq_adw_xref_contact_name():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_name`(
    contact_adw_key                  STRING       NOT NULL,
    contact_source_system_name       STRING    NOT NULL,
    name_type_code                   STRING    NOT NULL,
    effective_start_datetime         DATETIME           NOT NULL,
    name_adw_key                     STRING       NOT NULL,
    effective_end_datetime           DATETIME,
    active_indicator                 STRING,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_xref_contact_name', sql)
    return task

def bq_adw_xref_contact_phone():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.adw.xref_contact_phone`(
    contact_adw_key                  STRING       NOT NULL,
    contact_source_system_name       STRING    NOT NULL,
    phone_type_code                  STRING    NOT NULL,
    effective_start_datetime         DATETIME           NOT NULL,
    phone_adw_key                    STRING       NOT NULL,
    effective_end_datetime           DATETIME,
    active_indicator                 STRING        NOT NULL,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64        NOT NULL
)
;
"""

    task = int_u.run_query('adw_xref_contact_phone', sql)
    return task

def bq_insurance_insurance_agency():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_agency`(
    insurance_agency_adw_key    STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_agency', sql)
    return task

def bq_insurance_insurance_agent():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_agent`(
    insurance_agent_adw_key    STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_agent', sql)
    return task

def bq_insurance_insurance_client():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_client`(
    insurance_client_adw_key    STRING    NOT NULL,
    contact_adw_key            STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_client', sql)
    return task

def bq_insurance_insurance_coverage():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_coverage`(
    insurance_coverage_adw_key    STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_coverage', sql)
    return task

def bq_insurance_insurance_driver():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_driver`(
    insurance_driver_adw_key    STRING    NOT NULL,
    contact_adw_key            STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_driver', sql)
    return task

def bq_insurance_insurance_line():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_line`(
    insurance_policy_adw_key     STRING    NOT NULL,
    insurance_client_adw_key     STRING    NOT NULL,
    insurance_agent_adw_key      STRING    NOT NULL,
    insurance_agency_adw_key     STRING    NOT NULL,
    insurance_quote_adw_key      STRING    NOT NULL,
    insurance_driver_adw_key     STRING    NOT NULL,
    insurance_vehicle_adw_key    STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_line', sql)
    return task

def bq_insurance_insurance_monthly_policy_snapshot():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_monthly_policy_snapshot`(
    insurance_policy_monthly_snapshot_adw_key    STRING    NOT NULL,
    insurance_client_adw_key                     STRING    NOT NULL,
    insurance_agent_adw_key                      STRING    NOT NULL,
    insurance_agency_adw_key                     STRING    NOT NULL,
    insurance_coverage_adw_key                   STRING    NOT NULL,
    insurance_driver_adw_key                     STRING    NOT NULL,
    insurance_vehicle_adw_key                    STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_monthly_policy_snapshot', sql)
    return task

def bq_insurance_insurance_policy():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_policy`(
    insurance_policy_renewals_adw_key    STRING    NOT NULL,
    insurance_policy_adw_key             STRING    NOT NULL,
    insurance_agent_adw_key              STRING    NOT NULL,
    insurance_agency_adw_key             STRING    NOT NULL,
    insurance_client_adw_key             STRING,
    insurance_coverage_adw_key           STRING    NOT NULL,
    insurance_driver_adw_key             STRING,
    insurance_vehicle_adw_key            STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_policy', sql)
    return task

def bq_insurance_insurance_policy_transactions():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_policy_transactions`(
    insurance_policy_transactions_adw_key    STRING    NOT NULL,
    insurance_policy_renewals_adw_key        STRING    NOT NULL,
    insurance_client_adw_key                 STRING    NOT NULL,
    insurance_agent_adw_key                  STRING    NOT NULL,
    insurance_agency_adw_key                 STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_policy_transactions', sql)
    return task

def bq_insurance_insurance_quote():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_quote`(
    insurance_quote_adw_key       STRING    NOT NULL,
    insurance_client_adw_key      STRING    NOT NULL,
    insurance_agent_adw_key       STRING    NOT NULL,
    insurance_agency_adw_key      STRING    NOT NULL,
    insurance_coverage_adw_key    STRING    NOT NULL,
    insurance_driver_adw_key      STRING    NOT NULL,
    insurance_vehicle_adw_key     STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_quote', sql)
    return task

def bq_insurance_insurance_vehicle():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.insurance.insurance_vehicle`(
    insurance_vehicle_adw_key    STRING    NOT NULL
)
;
"""

    task = int_u.run_query('insurance_insurance_vehicle', sql)
    return task

def bq_member_dim_member():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_member`(
    member_adw_key                                  STRING    NOT NULL,
    membership_adw_key                              STRING    NOT NULL,
    contact_adw_key                                 STRING       NOT NULL,
    membership_solicitation_adw_key                 STRING    NOT NULL,
    employee_role_adw_key                           STRING    NOT NULL,
    member_source_system_key                        INT64,
    membership_identifier                           STRING,
    member_associate_identifier                     STRING,
    member_check_digit_number                       INT64,
    member_status_code                              STRING,
    member_expiration_date                          DATE,
    member_card_expiration_date                     DATE,
    primary_member_indicator                        STRING,
    member_do_not_renew_indicator                   STRING,
    member_billing_code                             STRING,
    member_billing_category_code                    STRING,
    member_status_datetime                          DATETIME,
    member_cancel_date                              DATE,
    member_join_aaa_date                            DATE,
    member_join_club_date                           DATE,
    member_reason_joined_code                       STRING,
    member_associate_relation_code                  STRING,
    member_solicitation_code                        STRING,
    member_source_of_sale_code                      STRING,
    member_commission_code                          STRING,
    member_future_cancel_indicator                  STRING,
    member_future_cancel_date                       DATE,
    member_renew_method_code                        STRING,
    free_member_indicator                           STRING,
    previous_club_membership_identifier             STRING,
    previous_club_code                              STRING,
    merged_member_previous_key                      INT64,
    merged_member_previous_club_code                STRING,
    merged_member_previous_membership_identifier    STRING,
    merged_member_previous_associate_identifier     STRING,
    merged_member_previous_check_digit_number       STRING,
    merged_member_previous_member16_identifier      STRING,
    member_name_change_datetime                     DATETIME,
    member_email_changed_datetime                   DATETIME,
    member_bad_email_indicator                      STRING,
    member_email_optout_indicator                   STRING,
    member_web_last_login_datetime                  DATETIME,
    member_active_expiration_date                   DATE,
    member_activation_date                          DATE,
    member_dn_text_indicator                        STRING,
    member_duplicate_email_indicator                STRING,
    member_dn_call_indicator                        STRING,
    member_no_email_indicator                       STRING,
    member_dn_mail_indicator                        STRING,
    member_dn_ask_for_email_indicator               STRING,
    member_dn_email_indicator                       STRING,
    member_refused_give_email_indicator             STRING,
    membership_purge_indicator                      STRING,
    effective_start_datetime                        DATETIME           NOT NULL,
    effective_end_datetime                          DATETIME           NOT NULL,
    active_indicator                                STRING        NOT NULL,
    adw_row_hash                                    STRING       NOT NULL,
    integrate_insert_datetime                       DATETIME           NOT NULL,
    integrate_insert_batch_number                   INT64        NOT NULL,
    integrate_update_datetime                       DATETIME           NOT NULL,
    integrate_update_batch_number                   INT64        NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_member', sql)
    return task

def bq_member_dim_member_auto_renewal_card():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card`(
    member_auto_renewal_card_adw_key    STRING    NOT NULL,
    name_adw_key                        STRING       NOT NULL,
    address_adw_key                     STRING       NOT NULL,
    member_source_arc_key               INT64,
    member_arc_status_datetime          DATETIME,
    member_cc_type_code                 STRING,
    member_cc_expiration_date           DATE,
    member_cc_reject_reason_code        STRING,
    member_cc_reject_date               DATE,
    member_cc_donor_number              STRING,
    member_cc_last_four                 STRING,
    member_credit_debit_type_code       STRING,
    effective_start_datetime            DATETIME           NOT NULL,
    effective_end_datetime              DATETIME           NOT NULL,
    active_indicator                    STRING        NOT NULL,
    adw_row_hash                        STRING       NOT NULL,
    integrate_insert_datetime           DATETIME           NOT NULL,
    integrate_insert_batch_number       INT64        NOT NULL,
    integrate_update_datetime           DATETIME           NOT NULL,
    integrate_update_batch_number       INT64        NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_member_auto_renewal_card', sql)
    return task

def bq_member_dim_member_rider():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_member_rider`(
    member_rider_adw_key                     STRING      NOT NULL,
    membership_adw_key                       STRING      NOT NULL,
    member_adw_key                           STRING      NOT NULL,
    member_auto_renewal_card_adw_key         STRING      NOT NULL,
    employee_role_adw_key                    STRING      NOT NULL,
    product_adw_key                          STRING      NOT NULL,
    member_rider_source_key                  INT64,
    member_rider_status_code                 STRING,
    member_rider_status_datetime             DATETIME,
    member_rider_cancel_date                 DATE,
    member_rider_cost_effective_date         DATE,
    member_rider_solicitation_code           STRING,
    member_rider_do_not_renew_indicator      STRING,
    member_rider_dues_cost_amount            NUMERIC,
    member_rider_dues_adjustment_amount      NUMERIC,
    member_rider_payment_amount              NUMERIC,
    member_rider_paid_by_code                STRING,
    member_rider_future_cancel_date          DATE,
    member_rider_billing_category_code       STRING,
    member_rider_cancel_reason_code          STRING,
    member_rider_reinstate_indicator         STRING,
    member_rider_effective_date              DATE,
    member_rider_adm_original_cost_amount    NUMERIC,
    member_rider_reinstate_reason_code       STRING,
    member_rider_extend_expiration_amount    NUMERIC,
    member_rider_actual_cancel_datetime      DATETIME,
    member_rider_definition_key              INT64,
    member_rider_activation_date             DATE,
    effective_start_datetime                 DATETIME             NOT NULL,
    effective_end_datetime                   DATETIME             NOT NULL,
    active_indicator                         STRING          NOT NULL,
    adw_row_hash                             STRING,
    integrate_insert_datetime                DATETIME             NOT NULL,
    integrate_insert_batch_number            INT64          NOT NULL,
    integrate_update_datetime                DATETIME             NOT NULL,
    integrate_update_batch_number            INT64          NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_member_rider', sql)
    return task


def bq_member_dim_membership():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_membership`(
    membership_adw_key                                       STRING      NOT NULL,
    address_adw_key                                          STRING         NOT NULL,
    phone_adw_key                                            STRING         NOT NULL,
    product_adw_key                                          STRING      NOT NULL,
    aca_office_adw_key                                       STRING      NOT NULL,
    membership_source_system_key                             INT64,
    membership_identifier                                    STRING,
    membership_status_code                                   STRING,
    membership_billing_code                                  STRING,
    membership_status_datetime                               DATETIME,
    membership_cancel_date                                   DATE,
    membership_billing_category_code                         STRING,
    membership_dues_cost_amount                              NUMERIC,
    membership_dues_adjustment_amount                        NUMERIC,
    membership_future_cancel_indicator                       STRING,
    membership_future_cancel_date                            DATE,
    membership_out_of_territory_code                         STRING,
    membership_address_change_datetime                       DATETIME,
    membership_cdx_export_update_datetime                    DATETIME,
    membership_tier_key                                      FLOAT64,
    membership_temporary_address_indicator                   STRING,
    previous_club_membership_identifier                      STRING,
    previous_club_code                                       STRING,
    membership_transfer_club_code                            STRING,
    membership_transfer_datetime                             DATETIME,
    merged_membership_previous_club_code                     STRING,
    merged_member_previous_membership_identifier             STRING,
    merged_member_previous_membership_key                    INT64,
    membership_dn_solicit_indicator                          STRING,
    membership_bad_address_indicator                         STRING,
    membership_dn_send_publications_indicator                STRING,
    membership_market_tracking_code                          STRING,
    membership_salvage_flag                                  STRING,
    membership_salvaged_indicator                            STRING,
    membership_dn_salvage_indicator                          STRING,
    membership_group_code                                    STRING,
    membership_type_code                                     STRING,
    membership_ebill_indicator                               STRING,
    membership_marketing_segmentation_code                   STRING,
    membership_promo_code                                    STRING,
    membership_phone_type_code                               STRING,
    membership_ers_abuser_indicator                          STRING,
    membership_dn_send_aaaworld_indicator                    STRING,
    membership_shadow_motorcycle_indicator                   STRING,
    membership_student_indicator                             STRING,
    membership_address_updated_by_ncoa_indicator             STRING,
    membership_assigned_retention_indicator                  STRING,
    membership_no_membership_promotion_mailings_indicator    STRING,
    membership_dn_telemarket_indicator                       STRING,
    membership_dn_offer_plus_indicator                       STRING,
    membership_receive_aaaworld_online_edition_indicator     STRING,
    membership_shadowrv_coverage_indicator                   STRING,
    membership_dn_direct_mail_solicit_indicator              STRING,
    membership_heavy_ers_indicator                           STRING,
    membership_purge_indicator                               STRING,
    effective_start_datetime                                 DATETIME             NOT NULL,
    effective_end_datetime                                   DATETIME             NOT NULL,
    active_indicator                                         STRING          NOT NULL,
    adw_row_hash                                             STRING         NOT NULL,
    integrate_insert_datetime                                DATETIME             NOT NULL,
    integrate_insert_batch_number                            INT64          NOT NULL,
    integrate_update_datetime                                DATETIME             NOT NULL,
    integrate_update_batch_number                            INT64          NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_membership', sql)
    return task


def bq_member_dim_membership_fee():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_membership_fee`(
    membership_fee_adw_key           STRING    NOT NULL,
    member_adw_key                   STRING    NOT NULL,
    product_adw_key                  STRING    NOT NULL,
    membership_fee_source_key        INT64,
    status                           STRING,
    fee_type                         STRING,
    fee_date                         DATE,
    waived_date                      DATE,
    waived_by                        INT64,
    donor_number                     STRING,
    waived_reason_code               STRING,
    effective_start_datetime         DATETIME           NOT NULL,
    effective_end_datetime           DATETIME           NOT NULL,
    active_indicator                 STRING        NOT NULL,
    adw_row_hash                     STRING       NOT NULL,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64        NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_membership_fee', sql)
    return task


def bq_member_dim_membership_marketing_segmentation():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_membership_marketing_segmentation`(
    membership_marketing_segmentation_adw_key    STRING     NOT NULL,
    membership_adw_key                           STRING     NOT NULL,
    member_expiration_date                       DATE,
    segmentation_test_group_indicator            STRING,
    segmentation_panel_code                      STRING,
    segmentation_control_panel_indicator         STRING,
    segmentation_commission_code                 STRING,
    segment_name                                 STRING,
    effective_start_datetime                     DATETIME            NOT NULL,
    effective_end_datetime                       DATETIME            NOT NULL,
    active_indicator                             STRING         NOT NULL,
    adw_row_hash                                 STRING        NOT NULL,
    integrate_insert_datetime                    DATETIME            NOT NULL,
    integrate_insert_batch_number                INT64         NOT NULL,
    integrate_update_datetime                    DATETIME            NOT NULL,
    integrate_update_batch_number                INT64         NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_membership_marketing_segmentation', sql)
    return task

def bq_member_dim_membership_payment_plan():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_membership_payment_plan`(
    membership_payment_plan_adw_key    STRING     NOT NULL,
    membership_adw_key                 STRING     NOT NULL,
    member_rider_adw_key               STRING     NOT NULL,
    payment_plan_source_key            INT64,
    payment_plan_name                  STRING,
    payment_plan_payment_count         INT64,
    payment_plan_months                INT64,
    paymentnumber                      INT64,
    payment_plan_status                STRING,
    payment_plan_charge_date           DATE,
    safety_fund_donation_indicator     STRING,
    effective_start_datetime           DATETIME            NOT NULL,
    effective_end_datetime             DATETIME            NOT NULL,
    active_indicator                   STRING         NOT NULL,
    adw_row_hash                       STRING        NOT NULL,
    integrate_insert_datetime          DATETIME            NOT NULL,
    integrate_insert_batch_number      INT64         NOT NULL,
    integrate_update_datetime          DATETIME            NOT NULL,
    integrate_update_batch_number      INT64         NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_membership_payment_plan', sql)
    return task

def bq_member_dim_membership_solicitation():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.dim_membership_solicitation`(
    membership_solicitation_adw_key              STRING    NOT NULL,
    membership_solicitation_source_system_key    INT64,
    solicitation_code                            STRING,
    solicitation_group_code                      STRING,
    member_type_code                             STRING,
    solicitation_campaign_code                   STRING,
    solicitation_category_code                   STRING,
    solicitation_discount_code                   STRING,
    effective_start_datetime                     DATETIME           NOT NULL,
    effective_end_datetime                       DATETIME           NOT NULL,
    active_indicator                             STRING        NOT NULL,
    adw_row_hash                                 STRING       NOT NULL,
    integrate_insert_datetime                    DATETIME           NOT NULL,
    integrate_insert_batch_number                INT64        NOT NULL,
    integrate_update_datetime                    DATETIME           NOT NULL,
    integrate_update_batch_number                INT64        NOT NULL
)
;
"""

    task = int_u.run_query('member_dim_membership_solicitation', sql)
    return task

def bq_member_membership_billing_detail():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.membership_billing_detail`(
    membership_billing_detail_adw_key     STRING      NOT NULL,
    membership_billing_summary_adw_key    STRING,
    member_adw_key                        STRING      NOT NULL,
    product_adw_key                       STRING      NOT NULL,
    bill_detail_source_key                INT64,
    bill_summary_source_key               INT64,
    bill_process_date                     DATE,
    bill_notice_number                    INT64,
    bill_type                             STRING,
    bill_detail_amount                    NUMERIC,
    bill_detail_text                      STRING,
    rider_billing_category_code           STRING,
    rider_billing_category_description    STRING,
    rider_solicitation_code               STRING,
    effective_start_datetime              DATETIME             NOT NULL,
    effective_end_datetime                DATETIME             NOT NULL,
    active_indicator                      STRING          NOT NULL,
    adw_row_hash                          STRING         NOT NULL,
    integrate_insert_datetime             DATETIME             NOT NULL,
    integrate_insert_batch_number         INT64          NOT NULL,
    integrate_update_datetime             DATETIME             NOT NULL,
    integrate_update_batch_number         INT64          NOT NULL
)
;
"""

    task = int_u.run_query('member_membership_billing_detail', sql)
    return task

def bq_member_membership_billing_summary():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary`(
    membership_billing_summary_adw_key    STRING      NOT NULL,
    membership_adw_key                    STRING      NOT NULL,
    bill_summary_source_key               INT64,
    bill_process_date                     DATE,
    bill_notice_number                    INT64,
    bill_amount                           NUMERIC,
    bill_credit_amount                    NUMERIC,
    bill_type                             STRING,
    member_expiration_date                DATE,
    bill_paid_amount                      NUMERIC,
    bill_renewal_method                   STRING,
    bill_panel_code                       STRING,
    bill_ebilling_indicator               STRING,
    effective_start_datetime              DATETIME             NOT NULL,
    effective_end_datetime                DATETIME             NOT NULL,
    active_indicator                      STRING          NOT NULL,
    adw_row_hash                          STRING         NOT NULL,
    integrate_insert_datetime             DATETIME             NOT NULL,
    integrate_insert_batch_number         INT64          NOT NULL,
    integrate_update_datetime             DATETIME             NOT NULL,
    integrate_update_batch_number         INT64          NOT NULL
)
;
"""

    task = int_u.run_query('member_membership_billing_summary', sql)
    return task

def bq_member_membership_gl_payments_applied():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.membership_gl_payments_applied`(
    membership_gl_payments_applied_adw_key       STRING      NOT NULL,
    membership_adw_key                           STRING,
    aca_office_adw_key                           STRING,
    gl_payment_journal_source_key                INT64,
    gl_payment_gl_description                    STRING,
    gl_payment_account_number                    STRING,
    gl_payment_post_date                         DATE,
    gl_payment_process_date                      DATE,
    gl_payment_journal_amount                    NUMERIC,
    gl_payment_journal_created_datetime          DATETIME,
    gl_payment_journal_debit_credit_indicator    STRING,
    effective_start_datetime                     DATETIME             NOT NULL,
    effective_end_datetime                       DATETIME             NOT NULL,
    active_indicator                             STRING         NOT NULL,
    adw_row_hash                                 STRING         NOT NULL,
    integrate_insert_datetime                    DATETIME             NOT NULL,
    integrate_insert_batch_number                INT64          NOT NULL,
    integrate_update_datetime                    DATETIME             NOT NULL,
    integrate_update_batch_number                INT64          NOT NULL
)
;
"""

    task = int_u.run_query('member_membership_gl_payments_applied', sql)
    return task

def bq_member_membership_payment_applied_detail():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_detail`(
    membership_payment_applied_detail_adw_key     STRING      NOT NULL,
    member_adw_key                                STRING      NOT NULL,
    membership_payment_applied_summary_adw_key    STRING      NOT NULL,
    product_adw_key                               STRING      NOT NULL,
    payment_applied_detail_source_key             INT64,
    payment_applied_date                          DATE,
    payment_method_code                           STRING,
    payment_method_description                    STRING,
    payment_detail_amount                         NUMERIC,
    payment_applied_unapplied_amount              NUMERIC,
    discount_amount                               NUMERIC,
    payment_applied_discount_counted              STRING,
    discount_effective_date                       DATE,
    rider_cost_effective_datetime                 DATETIME,
    effective_start_datetime                      DATETIME             NOT NULL,
    effective_end_datetime                        DATETIME             NOT NULL,
    active_indicator                              STRING          NOT NULL,
    adw_row_hash                                  STRING         NOT NULL,
    integrate_insert_datetime                     DATETIME             NOT NULL,
    integrate_insert_batch_number                 INT64          NOT NULL,
    integrate_update_datetime                     DATETIME             NOT NULL,
    integrate_update_batch_number                 INT64          NOT NULL
)
;
"""

    task = int_u.run_query('member_membership_payment_applied_detail', sql)
    return task

def bq_member_membership_payment_applied_summary():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.membership_payment_applied_summary`(
    membership_payment_applied_summary_adw_key     STRING       NOT NULL,
    membership_payment_applied_reversal_adw_key    STRING       NOT NULL,
    membership_adw_key                             STRING       NOT NULL,
    membership_payments_received_adw_key           STRING       NOT NULL,
    aca_office_adw_key                             STRING       NOT NULL,
    employee_adw_key                               STRING       NOT NULL,
    payment_applied_source_key                     INT64,
    payment_applied_date                           DATE,
    payment_applied_amount                         NUMERIC,
    payment_batch_name                             STRING,
    payment_method_code                            STRING,
    payment_method_description                     STRING,
    payment_type_code                              STRING,
    payment_type_description                       STRING,
    payment_adjustment_code                        STRING,
    payment_adjustment_description                 STRING,
    payment_created_datetime                       DATETIME,
    payment_paid_by_code                           STRING,
    payment_unapplied_amount                       NUMERIC,
    effective_start_datetime                       DATETIME              NOT NULL,
    effective_end_datetime                         DATETIME              NOT NULL,
    active_indicator                               STRING           NOT NULL,
    adw_row_hash                                   STRING          NOT NULL,
    integrate_insert_datetime                      DATETIME              NOT NULL,
    integrate_insert_batch_number                  INT64           NOT NULL,
    integrate_update_datetime                      DATETIME              NOT NULL,
    integrate_update_batch_number                  INT64           NOT NULL
)
;
"""
    task = int_u.run_query('member_membership_payment_applied_summary', sql)
    return task


def bq_member_membership_payments_received():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received`(
    membership_payments_received_adw_key    STRING       NOT NULL,
    membership_adw_key                      STRING,
    membership_billing_summary_adw_key      STRING       NOT NULL,
    member_auto_renewal_card_adw_key        STRING,
    aca_office_adw_key                      STRING       NOT NULL,
    employee_adw_key                        STRING       NOT NULL,
    payment_received_source_key             INT64,
    payment_received_batch_key              INT64,
    payment_received_name                   STRING,
    payment_received_count                  INT64,
    payment_received_expected_amount        NUMERIC,
    payment_received_created_datetime       DATETIME,
    payment_received_status                 STRING,
    payment_received_status_datetime        DATETIME,
    payment_received_amount                 NUMERIC,
    payment_received_method_code            STRING,
    payment_received_method_description     STRING,
    payment_source_code                     STRING,
    payment_source_description              STRING,
    payment_type_code                       STRING,
    payment_type_description                STRING,
    payment_reason_code                     STRING,
    payment_paid_by_description             STRING,
    payment_adjustment_code                 STRING,
    payment_adjustment_description          STRING,
    payment_received_post_flag              STRING,
    payment_received_post_datetime          DATETIME,
    payment_received_auto_renewal_flag      STRING,
    effective_start_datetime                DATETIME              NOT NULL,
    effective_end_datetime                  DATETIME              NOT NULL,
    active_indicator                       STRING           NOT NULL,
    adw_row_hash                            STRING,
    integrate_insert_datetime               DATETIME              NOT NULL,
    integrate_insert_batch_number           INT64           NOT NULL,
    integrate_update_datetime               DATETIME              NOT NULL,
    integrate_update_batch_number           INT64           NOT NULL
)
;
"""
    task = int_u.run_query('member_membership_payments_received', sql)
    return task


def bq_member_sales_agent_activity():
    sql = f"""
CREATE TABLE IF NOT EXISTS `{int_u.INTEGRATION_PROJECT}.member.sales_agent_activity`(
    sales_agent_activity_adw_key                         STRING      NOT NULL,
    member_adw_key                                       STRING      NOT NULL,
    membership_adw_key                                   STRING      NOT NULL,
    membership_solicitation_adw_key                      STRING      NOT NULL,
    aca_sale_office_adw_key                              STRING      NOT NULL,
    aca_membership_office_adw_key                        STRING      NOT NULL,
    employee_role_adw_key                                STRING      NOT NULL,
    product_adw_key                                      STRING      NOT NULL,
    membership_payment_applied_detail_adw_key            STRING      NOT NULL,
    sales_agent_activity_source_name                     STRING,
    sales_agent_activity_source_key                      INT64,
    sales_agent_activity_transaction_date                DATE,
    member_commission_code                               STRING,
    member_commission_description                        STRING,
    member_rider_code                                    STRING,
    member_rider_description                             STRING,
    member_expiration_date                               DATE,
    rider_solicitation_code                              STRING,
    rider_source_of_sale_code                            STRING,
    sales_agent_transaction_code                         STRING,
    sales_agent_activity_dues_amount                     NUMERIC,
    sales_agent_activity_daily_billed_indicator          STRING,
    rider_billing_category_code                          STRING,
    rider_billing_category_description                   STRING,
    member_type_code                                     STRING,
    sales_agent_activity_commissionable_activity_flag    STRING,
    sales_agent_activity_add_on_flag                     STRING,
    sales_agent_activity_auto_renewal_flag               STRING,
    sales_agent_activity_fee_type_code                   STRING,
    safety_fund_donation_indicator                       STRING,
    sales_agent_activity_payment_plan_flag               STRING,
    zip_code                                             STRING,
    sales_agent_activity_daily_count_indicator           STRING,
    daily_count                                          INT64,
    effective_start_datetime                             DATETIME             NOT NULL,
    effective_end_datetime                               DATETIME             NOT NULL,
    active_indicator                                     STRING          NOT NULL,
    adw_row_hash                                         STRING         NOT NULL,
    integrate_insert_datetime                            DATETIME             NOT NULL,
    integrate_insert_batch_number                        INT64          NOT NULL,
    integrate_update_datetime                            DATETIME             NOT NULL,
    integrate_update_batch_number                        INT64          NOT NULL
)
;
"""
    task = int_u.run_query('member_sales_agent_activity', sql)
    return task


def bq_adw_build_market_magnifier():
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{INGESTION_PROJECT}.demographic.marketing_magnifier` (
          CLUB                                                               STRING    ,
          MEMB_ID                                                            STRING    ,
          ASS_ID                                                             STRING    ,
          MEMTITLE                                                           STRING    ,
          NAMFIRST                                                           STRING    ,
          NAMEMID                                                            STRING    ,
          NAMLAST                                                            STRING    ,
          NAMEGEN                                                            STRING    ,
          NAMESUFF                                                           STRING    ,
          ADDRESS1                                                           STRING    ,
          ADD_SUP                                                            STRING    ,
          ADD_ATT                                                            STRING    ,
          CITY1                                                              STRING    ,
          STATE1                                                             STRING    ,
          ZIP1                                                               STRING    ,
          ZIP1_4                                                             STRING    ,
          BASEYR                                                             STRING    ,
          STATUS                                                             STRING    ,
          CANCELDT                                                           STRING    ,
          SEGMENT                                                            STRING    ,
          JOINYR                                                             STRING    ,
          EXPIREDT                                                           STRING    ,
          XMEMPHONE                                                          STRING    ,
          SOLIC_DM                                                           STRING    ,
          SOLIC_TM                                                           STRING    ,
          BIRTHDAT                                                           STRING    ,
          TRANSFER                                                           STRING    ,
          MEMBTYPE                                                           STRING    ,
          MEMBPLUS                                                           STRING    ,
          BAILABUS                                                           STRING    ,
          RTNCHECK                                                           STRING    ,
          REINSTAT                                                           STRING    ,
          SOURCE                                                             STRING    ,
          HH                                                                 STRING    ,
          MEMBER                                                             STRING    ,
          address_id                                                         STRING    ,
          address_quality_indicator                                          STRING    ,
          county_code                                                        STRING    ,
          county_name                                                        STRING    ,
          latitude                                                           STRING    ,
          longitude                                                          STRING    ,
          level_latitude_longitude                                           STRING    ,
          uid                                                                STRING    ,
          phone_pander_flag                                                  STRING    ,
          unscrubbed_do_not_use_phone                                        STRING    ,
          special_usage_flag                                                 STRING    ,
          pander_source_flags                                                STRING    ,
          dwelling_unit_size                                                 STRING    ,
          dwelling_type_drv                                                  STRING    ,
          homeowner_probability_model                                        STRING    ,
          ownership_code_drv                                                 STRING    ,
          move_update_code                                                   STRING    ,
          move_update_date                                                   STRING    ,
          cppm_mail_order_buyer                                              STRING    ,
          dma_pander_flag                                                    STRING    ,
          home_business                                                      STRING    ,
          lor_drv_numeric                                                    STRING    ,
          number_of_adults_in_household                                      STRING    ,
          rural_urban_county_size_code                                       STRING    ,
          last_activity_date_of_lu                                           STRING    ,
          pid                                                                STRING    ,
          name_type                                                          STRING    ,
          first_name                                                         STRING    ,
          middle_initial                                                     STRING    ,
          last_name                                                          STRING    ,
          surname_suffix_p1                                                  STRING    ,
          ethnic_detail_code                                                 STRING    ,
          ethnic_religion_code                                               STRING    ,
          ethnic_language_pref                                               STRING    ,
          ethnic_grouping_code                                               STRING    ,
          ethnic_country_of_origin_code                                      STRING    ,
          gender_code_drv                                                    STRING    ,
          exact_dob_drv_orig                                                 STRING    ,
          estimated_age_p1                                                   STRING    ,
          exact_age_p1                                                       STRING    ,
          combined_age_indicator                                             STRING    ,
          age_in_years_drv_numeric                                           STRING    ,
          education_p1                                                       STRING    ,
          marital_status_confidence_flag                                     STRING    ,
          marital_code_drv                                                   STRING    ,
          occupation_code_polk                                               STRING    ,
          business_owner_p1                                                  STRING    ,
          number_children_drv                                                STRING    ,
          mor_bank_upscale_merch_buyer                                       STRING    ,
          mor_bank_male_merch_buyer                                          STRING    ,
          mor_bank_female_merch_buyer                                        STRING    ,
          mor_bank_crafts_hobb_merch_buyer                                   STRING    ,
          mor_bank_gardening_farming_buyer                                   STRING    ,
          mor_bank_book_buyer                                                STRING    ,
          mor_bank_collect_special_foods_buyer                               STRING    ,
          mor_bank_gifts_and_gadgets_buyer                                   STRING    ,
          mor_bank_general_merch_buyer                                       STRING    ,
          mor_bank_family_and_general_magazine                               STRING    ,
          mor_bank_female_oriented_magazine                                  STRING    ,
          mor_bank_male_sports_magazine                                      STRING    ,
          mor_bank_religious_magazine                                        STRING    ,
          mor_bank_gardening_farming_magazine                                STRING    ,
          mor_bank_culinary_interests_magazine                               STRING    ,
          mor_bank_health_and_fitness_magazine                               STRING    ,
          mor_bank_do_it_yourselfers                                         STRING    ,
          mor_bank_news_and_financial                                        STRING    ,
          mor_bank_photography                                               STRING    ,
          mor_bank_opportunity_seekers_and_ce                                STRING    ,
          mor_bank_religious_contributor                                     STRING    ,
          mor_bank_political_contributor                                     STRING    ,
          mor_bank_health_and_institution_contributor                        STRING    ,
          mor_bank_general_contributor                                       STRING    ,
          mor_bank_miscellaneous                                             STRING    ,
          mor_bank_odds_and_ends                                             STRING    ,
          mor_bank_deduped_category_hit_count                                STRING    ,
          mor_bank_non_deduped_category_hit_count                            STRING    ,
          home_purch_price                                                   STRING    ,
          home_purch_date_char                                               STRING    ,
          year_built_fam2                                                    STRING    ,
          home_land_value                                                    STRING    ,
          home_property_ind                                                  STRING    ,
          year_built_new_confidence_flag                                     STRING    ,
          eff_year_built                                                     STRING    ,
          donates_to_environmental_causes                                    STRING    ,
          contributes_to_charities                                           STRING    ,
          presence_of_credit_card                                            STRING    ,
          presence_of_premium_credit_card                                    STRING    ,
          interest_in_reading                                                STRING    ,
          computer_owner                                                     STRING    ,
          mosaic_household_2011                                              STRING    ,
          mosaic_z4_2011                                                     STRING    ,
          mosaic_household_global_2011                                       STRING    ,
          mosaic_z4_global_2011                                              STRING    ,
          household_composition                                              STRING    ,
          core_based_statistical_areas_cbsa                                  STRING    ,
          core_based_statistical_area_type                                   STRING    ,
          home_total_value                                                   STRING    ,
          total_tax                                                          STRING    ,
          home_improvement_value                                             STRING    ,
          home_swimming_pool_indicator                                       STRING    ,
          home_base_square_footage                                           STRING    ,
          interest_in_gardening                                              STRING    ,
          interest_in_automotive                                             STRING    ,
          interest_in_gourmet_cooking                                        STRING    ,
          home_decorating_furnishing                                         STRING    ,
          dog_enthusiasts                                                    STRING    ,
          cat_enthusiast                                                     STRING    ,
          pet_enthusiast                                                     STRING    ,
          interest_in_travel                                                 STRING    ,
          interest_in_fitness                                                STRING    ,
          interest_in_the_outdoors                                           STRING    ,
          interest_in_sports                                                 STRING    ,
          investor_flag                                                      STRING    ,
          purchased_through_the_mail                                         STRING    ,
          cruise_enthusiasts                                                 STRING    ,
          invest_in_mutual_funds_annuities                                   STRING    ,
          internet_online_subscriber                                         STRING    ,
          purchase_via_on_line                                               STRING    ,
          interest_in_domestic_travel                                        STRING    ,
          interest_in_foreign_travel                                         STRING    ,
          type_of_purchase                                                   STRING    ,
          home_mortgage_amt                                                  STRING    ,
          mortgage_lender_name                                               STRING    ,
          mortgage_rate_type                                                 STRING    ,
          mortgage_term_in_months                                            STRING    ,
          home_mortgage_type                                                 STRING    ,
          deed_date_of_equity_loan                                           STRING    ,
          equity_amount_in_thousands                                         STRING    ,
          equity_lender_name                                                 STRING    ,
          equity_rate_type                                                   STRING    ,
          equity_term                                                        STRING    ,
          equity_loan_type                                                   STRING    ,
          deed_date_of_refi_loan                                             STRING    ,
          refi_mortgage_amt                                                  STRING    ,
          refinance_lender_name                                              STRING    ,
          refinance_rate_type                                                STRING    ,
          refinance_term_in_months                                           STRING    ,
          refinance_loan_type                                                STRING    ,
          landlord_flag                                                      STRING    ,
          pcsc_scrxplus_p50                                                  STRING    ,
          mortgage_home_purchase_est_current_mortgage_amount_new             STRING    ,
          mortgage_home_purchase_est_current_monthly_mortgage_payment_new    STRING    ,
          enh_ltv_drv_confidence_flag                                        STRING    ,
          enh_ltv_drv                                                        STRING    ,
          mortgage_home_purchase_est_available_equity_new                    STRING    ,
          children_0_18_v3                                                   STRING    ,
          children_0_3_v3                                                    STRING    ,
          children_0_3_gender                                                STRING    ,
          children_4_6_v3                                                    STRING    ,
          children_4_6_gender                                                STRING    ,
          children_7_9_v3                                                    STRING    ,
          children_7_9_gender                                                STRING    ,
          children_10_12_v3                                                  STRING    ,
          children_10_12_gender                                              STRING    ,
          children_13_15_v3                                                  STRING    ,
          children_13_15_gender                                              STRING    ,
          children_16_18_v3                                                  STRING    ,
          children_16_18_gender                                              STRING    ,
          ace_prim_addr                                                      STRING    ,
          ace_sec_addr                                                       STRING    ,
          city                                                               STRING    ,
          state                                                              STRING    ,
          zip                                                                STRING    ,
          zip4                                                               STRING    ,
          ace_prim_range                                                     STRING    ,
          ace_predir                                                         STRING    ,
          ace_prim_name                                                      STRING    ,
          ace_suffix                                                         STRING    ,
          ace_postdir                                                        STRING    ,
          ace_unit_desig                                                     STRING    ,
          ace_sec_range                                                      STRING    ,
          ace_rr_number                                                      STRING    ,
          ace_rr_box                                                         STRING    ,
          ace_z4_record_type                                                 STRING    ,
          ace_county                                                         STRING    ,
          dpbc_check_digit                                                   STRING    ,
          dpbc                                                               STRING    ,
          carrier_rt_code                                                    STRING    ,
          lot                                                                STRING    ,
          lot_order                                                          STRING    ,
          ace_addr_numerics                                                  STRING    ,
          hisp_score_drv                                                     STRING    ,
          slpi_drv                                                           STRING    ,
          wealth                                                             STRING    ,
          wealth_state                                                       STRING    ,
          birth_month                                                        STRING    ,
          mortgage_age                                                       STRING    ,
          home_purch_month                                                   STRING    ,
          exact_dob_mm_yyyy                                                  STRING    ,
          refi_flag_txn                                                      STRING    ,
          phone_drv                                                          STRING    ,
          phone_flag_drv                                                     STRING    ,
          prd_auto_month                                                     STRING    ,
          prd_prop_month                                                     STRING    ,
          children_age_16_17                                                 STRING    ,
          presence_of_boat                                                   STRING    ,
          motorcycle_ownership_code                                          STRING    ,
          recreational_vehicle_ownership_code                                STRING    ,
          number_of_vehicles_registered                                      STRING    ,
          recipient_reliability_code                                         STRING    ,
          annuity_score                                                      STRING    ,
          marketing_risk_index                                               STRING    ,
          hispanic_percentage                                                STRING    ,
          activity_tile                                                      STRING    ,
          score_resp_final_expense                                           STRING    ,
          score_resp_med_sup                                                 STRING    ,
          bk_score                                                           STRING    ,
          age_16_24_in_hh                                                    STRING    ,
          apt_flag                                                           STRING    ,
          sfdu_flag_drv                                                      STRING    ,
          presence_boat_motorcycle_rv                                        STRING    ,
          rural_flag_drv                                                     STRING    ,
          home_lender_name                                                   STRING    ,
          home_purch_date                                                    STRING    ,
          lexid                                                              STRING    ,
          mortgage_protection_model                                          STRING    ,
          life_attrition_model                                               STRING    ,
          life_attrition_model_decile                                        STRING    ,
          enh_mktval_drv                                                     STRING    ,
          annuity_decile                                                     STRING    ,
          fe_tier                                                            STRING    ,
          bk_tier                                                            STRING    ,
          tier_medsup                                                        STRING    ,
          lexhhid                                                            STRING    ,
          mri_tier_trv                                                       STRING    ,
          mortgage_protection_decile                                         STRING    ,
          truetouch_buyamerican                                              STRING    ,
          truetouch_showmethemoney                                           STRING    ,
          truetouch_gowiththeflow                                            STRING    ,
          truetouch_notimelikethepresent                                     STRING    ,
          truetouch_nevershowupemptyhanded                                   STRING    ,
          truetouch_ontheroadagain                                           STRING    ,
          truetouch_lookatmenow                                              STRING    ,
          truetouch_stopandsmelltheroses                                     STRING    ,
          truetouch_workhardplayhard                                         STRING    ,
          truetouch_apennysavedapennyearned                                  STRING    ,
          truetouch_itsallinthename                                          STRING    ,
          state_code                                                         STRING    ,
          title_of_respect_p1                                                STRING    ,
          boca_confidence_score                                              STRING    ,
          number_of_persons_in_living_unit                                   STRING    ,
          auto_in_the_market_new                                             STRING    ,
          auto_in_the_market_used                                            STRING    ,
          auto_in_the_market_used_0_5                                        STRING    ,
          auto_in_the_market_used_6_10                                       STRING    ,
          auto_in_the_market_used_11_plus                                    STRING    ,
          [2010_census_tract]                                                STRING    ,
          [2010_census_block]                                                STRING    ,
          [2010_census_median_age]                                           STRING    ,
          [2010_census_pct_population_age_under_18]                          STRING    ,
          [2010_census_pct_population_age_18plus]                            STRING    ,
          [2010_census_pct_population_age_65plus]                            STRING    ,
          [2010_census_pct_white_only]                                       STRING    ,
          [2010_census_pct_black_only]                                       STRING    ,
          [2010_census_pct_asian_only]                                       STRING    ,
          [2010_census_pct_hispanic]                                         STRING    ,
          [2010_census_persons_per_household]                                STRING    ,
          [2010_census_average_household_size]                               STRING    ,
          [2010_census_pct_households_married_couples]                       STRING    ,
          [2010_census_pct_households_with_children]                         STRING    ,
          [2010_census_married_couple_families_with_any_person_under18]      STRING    ,
          [2010_census_married_couple_families_wo_any_person_under18]        STRING    ,
          [2010_census_pct_households_spanish_speaking]                      STRING    ,
          [2010_census_pct_25plus_median_years_school_completed]             STRING    ,
          [2010_census_pct_25plus_with_a_degree]                             STRING    ,
          [2010_census_median_housing_value]                                 STRING    ,
          [2010_census_units_in_structure_mobile_home]                       STRING    ,
          [2010_census_median_dwelling_age]                                  STRING    ,
          [2010_census_pct_dwelling_units_owner_occupied]                    STRING    ,
          [2010_census_pct_dwelling_units_renter_occupied]                   STRING    ,
          [2010_census_ispsa]                                                STRING    ,
          [2010_census_wealth_rating]                                        STRING    ,
          [2010_census_state_median_family_income]                           STRING    ,
          [2010_census_income_index]                                         STRING    ,
          est_hh_income_v5                                                   STRING    ,
          occupation_group_p1_v2                                             STRING    ,
          channel_preference                                                 STRING    ,
          marketing_attract_index                                            STRING    ,
          premium_decile                                                     STRING    ,
          prospectsurvival_decile                                            STRING    ,
          cfi_household_deposits_score                                       STRING    ,
          cfi_investable_assets_score                                        STRING    ,
          cfi_investment_balances_score                                      STRING    ,
          cfi_mortgage_refinance_score                                       STRING    ,
          cfi_net_asset_score                                                STRING    ,
          marketing_attract_score                                            STRING    ,
          truetouch_broadcastcabletv                                         STRING    ,
          truetouch_directmail                                               STRING    ,
          truetouch_email                                                    STRING    ,
          truetouch_internetradio                                            STRING    ,
          truetouch_mobiledisplay                                            STRING    ,
          truetouch_mobilevideo                                              STRING    ,
          truetouch_onlinevideo                                              STRING    ,
          truetouch_onlinedisplay                                            STRING    ,
          truetouch_onlinestreamingtv                                        STRING    ,
          truetouch_satelliteradio                                           STRING    ,
          truetouch_brickmortar                                              STRING    ,
          truetouch_onlinemidhigh                                            STRING    ,
          truetouch_etailer                                                  STRING    ,
          truetouch_onlinediscount                                           STRING    ,
          truetouch_onlinebidmrktplc                                         STRING    ,
          territory_split                                                    STRING    ,
          club_code                                                          STRING    ,
          territory_sequence                                                 STRING    ,
          source_code                                                        STRING    ,
          number_of_sources                                                  STRING    ,
          mailable_flag                                                      STRING    ,
          exclusion_code                                                     STRING    ,
          dt_last_seen                                                       STRING    ,
          time_zone                                                          STRING    ,
          second_phone_number                                                STRING    ,
          unscrubbed_do_not_use_areacode                                     STRING    ,
          estimated_current_home_value_rest                                  STRING    ,
          geo_lat                                                            STRING    ,
          geo_long                                                           STRING    ,
          geo_blk                                                            STRING    ,
          geo_match                                                          STRING    ,
          msa                                                                STRING    ,
          cr_sort_sz                                                         STRING    ,
          v_city_name                                                        STRING    ,
          p_city_name                                                        STRING    ,
          scf_drv                                                            STRING    ,
          state_county_lookup_numeric                                        STRING    ,
          citykey_drv                                                        STRING    ,
          zip9                                                               STRING    ,
          home_land_square_footage                                           STRING    ,
          home_land_front_footage                                            STRING    ,
          home_land_depth_footage                                            STRING    ,
          home_stories                                                       STRING    ,
          home_total_rooms                                                   STRING    ,
          home_building_square_footage                                       STRING    ,
          home_bedrooms                                                      STRING    ,
          home_bath                                                          STRING    ,
          home_fireplaces                                                    STRING    ,
          home_floor_cover_indicator                                         STRING    ,
          home_heat_indicator                                                STRING    ,
          home_air_conditioning                                              STRING    ,
          home_exterior_wall_type                                            STRING    ,
          home_building_construction_indicator                               STRING    ,
          down_payment_pct                                                   STRING    ,
          zip_code2                                                          STRING    ,
          fips_1990_state_code                                               STRING    ,
          primary_house_number                                               STRING    ,
          street_pre_directional                                             STRING    ,
          street_name2                                                       STRING    ,
          street_suffix2                                                     STRING    ,
          street_post_directional                                            STRING    ,
          secondary_unit_designator_number                                   STRING    ,
          secondary_unit_designator                                          STRING    ,
          city_name2                                                         STRING    ,
          type_of_investment                                                 STRING    ,
          date_of_warranty_deed                                              STRING    ,
          purchase_amount_in_thousands                                       STRING    ,
          type_of_purchase2                                                  STRING    ,
          mortgage_amount_in_thousands2                                      STRING    ,
          home_lender_name_old                                               STRING    ,
          mortgage_rate_type2                                                STRING    ,
          dl_cppm_mortgage_term                                              STRING    ,
          i_home_mortgage_type                                               STRING    ,
          deed_date_of_equity_loan2                                          STRING    ,
          equity_amount_in_thousands2                                        STRING    ,
          equity_lender_name2                                                STRING    ,
          equity_rate_type2                                                  STRING    ,
          equity_term_in_months                                              STRING    ,
          equity_loan_type2                                                  STRING    ,
          deed_date_of_refinance_loan2                                       STRING    ,
          refinance_amount_in_thousands2                                     STRING    ,
          refinance_lender_name2                                             STRING    ,
          refinance_rate_type2                                               STRING    ,
          refinance_term                                                     STRING    ,
          refinance_loan_type2                                               STRING    ,
          dns_flag                                                           STRING    ,
          prd_confidence_code                                                STRING    ,
          age_drv                                                            STRING    ,
          presence_19_in_hh                                                  STRING    ,
          presence_16_19_yr_old                                              STRING    ,
          children_0_3_score_v3                                              STRING    ,
          children_4_6_score_v3                                              STRING    ,
          children_7_9_score_v3                                              STRING    ,
          children_10_12_score_v3                                            STRING    ,
          children_13_15_score_v3                                            STRING    ,
          children_16_18_score_v3                                            STRING    ,
          child_flag_drv                                                     STRING    ,
          children_0_3                                                       STRING    ,
          children_0_3_flag                                                  STRING    ,
          children_10_12                                                     STRING    ,
          children_10_12_flag                                                STRING    ,
          children_13_18                                                     STRING    ,
          children_13_18_flag                                                STRING    ,
          children_4_6                                                       STRING    ,
          children_4_6_flag                                                  STRING    ,
          children_7_9                                                       STRING    ,
          children_7_9_flag                                                  STRING    ,
          children_age_0_2                                                   STRING    ,
          exact_dob_drv                                                      STRING    ,
          channel_pref_i_decile                                              STRING    ,
          channel_pref_d_decile                                              STRING    ,
          channel_pref_e_decile                                              STRING    ,
          bodily_injury_limits_tier                                          STRING    ,
          collision_deductible_tier                                          STRING    ,
          life_classification_decile                                         STRING    ,
          coverage_tier                                                      STRING    ,
          group_code                                                         STRING    ,
          combined_age                                                       STRING    ,
          combined_dob                                                       STRING    ,
          combined_dwelling_type                                             STRING    ,
          combined_gender                                                    STRING    ,
          combined_length_of_residence                                       STRING    ,
          combined_marital_status                                            STRING    ,
          combined_ownership_code                                            STRING    ,
          combined_presence_of_children                                      STRING    ,
          combined_time_zone                                                 STRING    ,
          combined_phone                                                     STRING    ,
          combined_number_children                                           STRING    ,
          v1_donotmail                                                       STRING    ,
          v1_verifiedprospectfound                                           STRING    ,
          v1_verifiedname                                                    STRING    ,
          v1_verifiedssn                                                     STRING    ,
          v1_verifiedphone                                                   STRING    ,
          v1_verifiedcurrresmatchindex                                       STRING    ,
          v1_prospecttimeonrecord                                            STRING    ,
          v1_prospecttimelastupdate                                          STRING    ,
          v1_prospectlastupdate12mo                                          STRING    ,
          v1_prospectage                                                     STRING    ,
          v1_prospectgender                                                  STRING    ,
          v1_prospectmaritalstatus                                           STRING    ,
          v1_prospectestimatedincomerange                                    STRING    ,
          v1_prospectdeceased                                                STRING    ,
          v1_prospectcollegeattending                                        STRING    ,
          v1_prospectcollegeattended                                         STRING    ,
          v1_prospectcollegeprogramtype                                      STRING    ,
          v1_prospectcollegeprivate                                          STRING    ,
          v1_prospectcollegetier                                             STRING    ,
          v1_prospectbankingexperience                                       STRING    ,
          v1_assetcurrowner                                                  STRING    ,
          v1_propcurrowner                                                   STRING    ,
          v1_propcurrownedcnt                                                STRING    ,
          v1_propcurrownedassessedttl                                        STRING    ,
          v1_propcurrownedavmttl                                             STRING    ,
          v1_proptimelastsale                                                STRING    ,
          v1_propeverownedcnt                                                STRING    ,
          v1_propeversoldcnt                                                 STRING    ,
          v1_propsoldcnt12mo                                                 STRING    ,
          v1_propsoldratio                                                   STRING    ,
          v1_proppurchasecnt12mo                                             STRING    ,
          v1_ppcurrowner                                                     STRING    ,
          v1_ppcurrownedcnt                                                  STRING    ,
          v1_ppcurrownedautocnt                                              STRING    ,
          v1_ppcurrownedmtrcyclecnt                                          STRING    ,
          v1_ppcurrownedaircrftcnt                                           STRING    ,
          v1_ppcurrownedwtrcrftcnt                                           STRING    ,
          v1_lifeevtimelastmove                                              STRING    ,
          v1_lifeevnamechange                                                STRING    ,
          v1_lifeevnamechangecnt12mo                                         STRING    ,
          v1_lifeevtimefirstassetpurchase                                    STRING    ,
          v1_lifeevtimelastassetpurchase                                     STRING    ,
          v1_lifeeveverresidedcnt                                            STRING    ,
          v1_lifeevlastmovetaxratiodiff                                      STRING    ,
          v1_lifeevecontrajectory                                            STRING    ,
          v1_lifeevecontrajectoryindex                                       STRING    ,
          v1_rescurrownershipindex                                           STRING    ,
          v1_rescurravmvalue                                                 STRING    ,
          v1_rescurravmvalue12mo                                             STRING    ,
          v1_rescurravmratiodiff12mo                                         STRING    ,
          v1_rescurravmvalue60mo                                             STRING    ,
          v1_rescurravmratiodiff60mo                                         STRING    ,
          v1_rescurravmcntyratio                                             STRING    ,
          v1_rescurravmtractratio                                            STRING    ,
          v1_rescurravmblockratio                                            STRING    ,
          v1_rescurrdwelltype                                                STRING    ,
          v1_rescurrdwelltypeindex                                           STRING    ,
          v1_rescurrmortgagetype                                             STRING    ,
          v1_rescurrmortgageamount                                           STRING    ,
          v1_rescurrbusinesscnt                                              STRING    ,
          v1_resinputownershipindex                                          STRING    ,
          v1_resinputavmvalue                                                STRING    ,
          v1_resinputavmvalue12mo                                            STRING    ,
          v1_resinputavmratiodiff12mo                                        STRING    ,
          v1_resinputavmvalue60mo                                            STRING    ,
          v1_resinputavmratiodiff60mo                                        STRING    ,
          v1_resinputavmcntyratio                                            STRING    ,
          v1_resinputavmtractratio                                           STRING    ,
          v1_resinputavmblockratio                                           STRING    ,
          v1_resinputdwelltype                                               STRING    ,
          v1_resinputdwelltypeindex                                          STRING    ,
          v1_resinputmortgagetype                                            STRING    ,
          v1_resinputmortgageamount                                          STRING    ,
          v1_resinputbusinesscnt                                             STRING    ,
          v1_crtreccnt                                                       STRING    ,
          v1_crtreccnt12mo                                                   STRING    ,
          v1_crtrectimenewest                                                STRING    ,
          v1_crtrecfelonycnt                                                 STRING    ,
          v1_crtrecfelonycnt12mo                                             STRING    ,
          v1_crtrecfelonytimenewest                                          STRING    ,
          v1_crtrecmsdmeancnt                                                STRING    ,
          v1_crtrecmsdmeancnt12mo                                            STRING    ,
          v1_crtrecmsdmeantimenewest                                         STRING    ,
          v1_crtrecevictioncnt                                               STRING    ,
          v1_crtrecevictioncnt12mo                                           STRING    ,
          v1_crtrecevictiontimenewest                                        STRING    ,
          v1_crtreclienjudgcnt                                               STRING    ,
          v1_crtreclienjudgcnt12mo                                           STRING    ,
          v1_crtreclienjudgtimenewest                                        STRING    ,
          v1_crtreclienjudgamtttl                                            STRING    ,
          v1_crtrecbkrptcnt                                                  STRING    ,
          v1_crtrecbkrptcnt12mo                                              STRING    ,
          v1_crtrecbkrpttimenewest                                           STRING    ,
          v1_crtrecseverityindex                                             STRING    ,
          v1_occproflicense                                                  STRING    ,
          v1_occproflicensecategory                                          STRING    ,
          v1_occbusinessassociation                                          STRING    ,
          v1_occbusinessassociationtime                                      STRING    ,
          v1_occbusinesstitleleadership                                      STRING    ,
          v1_interestsportperson                                             STRING    ,
          v1_hhteenagermmbrcnt                                               STRING    ,
          v1_hhyoungadultmmbrcnt                                             STRING    ,
          v1_hhmiddleagemmbrcnt                                              STRING    ,
          v1_hhseniormmbrcnt                                                 STRING    ,
          v1_hhelderlymmbrcnt                                                STRING    ,
          v1_hhcnt                                                           STRING    ,
          v1_hhestimatedincomerange                                          STRING    ,
          v1_hhcollegeattendedmmbrcnt                                        STRING    ,
          v1_hhcollege2yrattendedmmbrcnt                                     STRING    ,
          v1_hhcollege4yrattendedmmbrcnt                                     STRING    ,
          v1_hhcollegegradattendedmmbrcnt                                    STRING    ,
          v1_hhcollegeprivatemmbrcnt                                         STRING    ,
          v1_hhcollegetiermmbrhighest                                        STRING    ,
          v1_hhpropcurrownermmbrcnt                                          STRING    ,
          v1_hhpropcurrownedcnt                                              STRING    ,
          v1_hhpropcurravmhighest                                            STRING    ,
          v1_hhppcurrownedcnt                                                STRING    ,
          v1_hhppcurrownedautocnt                                            STRING    ,
          v1_hhppcurrownedmtrcyclecnt                                        STRING    ,
          v1_hhppcurrownedaircrftcnt                                         STRING    ,
          v1_hhppcurrownedwtrcrftcnt                                         STRING    ,
          v1_hhcrtrecmmbrcnt                                                 STRING    ,
          v1_hhcrtrecmmbrcnt12mo                                             STRING    ,
          v1_hhcrtrecfelonymmbrcnt                                           STRING    ,
          v1_hhcrtrecfelonymmbrcnt12mo                                       STRING    ,
          v1_hhcrtrecmsdmeanmmbrcnt                                          STRING    ,
          v1_hhcrtrecmsdmeanmmbrcnt12mo                                      STRING    ,
          v1_hhcrtrecevictionmmbrcnt                                         STRING    ,
          v1_hhcrtrecevictionmmbrcnt12mo                                     STRING    ,
          v1_hhcrtreclienjudgmmbrcnt                                         STRING    ,
          v1_hhcrtreclienjudgmmbrcnt12mo                                     STRING    ,
          v1_hhcrtreclienjudgamtttl                                          STRING    ,
          v1_hhcrtrecbkrptmmbrcnt                                            STRING    ,
          v1_hhcrtrecbkrptmmbrcnt12mo                                        STRING    ,
          v1_hhcrtrecbkrptmmbrcnt24mo                                        STRING    ,
          v1_hhoccproflicmmbrcnt                                             STRING    ,
          v1_hhoccbusinessassocmmbrcnt                                       STRING    ,
          v1_hhinterestsportpersonmmbrcnt                                    STRING    ,
          v1_raateenagemmbrcnt                                               STRING    ,
          v1_raayoungadultmmbrcnt                                            STRING    ,
          v1_raamiddleagemmbrcnt                                             STRING    ,
          v1_raaseniormmbrcnt                                                STRING    ,
          v1_raaelderlymmbrcnt                                               STRING    ,
          v1_raahhcnt                                                        STRING    ,
          v1_raammbrcnt                                                      STRING    ,
          v1_raamedincomerange                                               STRING    ,
          v1_raacollegeattendedmmbrcnt                                       STRING    ,
          v1_raacollege2yrattendedmmbrcnt                                    STRING    ,
          v1_raacollege4yrattendedmmbrcnt                                    STRING    ,
          v1_raacollegegradattendedmmbrcnt                                   STRING    ,
          v1_raacollegeprivatemmbrcnt                                        STRING    ,
          v1_raacollegetoptiermmbrcnt                                        STRING    ,
          v1_raacollegemidtiermmbrcnt                                        STRING    ,
          v1_raacollegelowtiermmbrcnt                                        STRING    ,
          v1_raapropcurrownermmbrcnt                                         STRING    ,
          v1_raapropowneravmhighest                                          STRING    ,
          v1_raapropowneravmmed                                              STRING    ,
          v1_raappcurrownermmbrcnt                                           STRING    ,
          v1_raappcurrownerautommbrcnt                                       STRING    ,
          v1_raappcurrownermtrcyclemmbrcnt                                   STRING    ,
          v1_raappcurrowneraircrftmmbrcnt                                    STRING    ,
          v1_raappcurrownerwtrcrftmmbrcnt                                    STRING    ,
          v1_raacrtrecmmbrcnt                                                STRING    ,
          v1_raacrtrecmmbrcnt12mo                                            STRING    ,
          v1_raacrtrecfelonymmbrcnt                                          STRING    ,
          v1_raacrtrecfelonymmbrcnt12mo                                      STRING    ,
          v1_raacrtrecmsdmeanmmbrcnt                                         STRING    ,
          v1_raacrtrecmsdmeanmmbrcnt12mo                                     STRING    ,
          v1_raacrtrecevictionmmbrcnt                                        STRING    ,
          v1_raacrtrecevictionmmbrcnt12mo                                    STRING    ,
          v1_raacrtreclienjudgmmbrcnt                                        STRING    ,
          v1_raacrtreclienjudgmmbrcnt12mo                                    STRING    ,
          v1_raacrtreclienjudgamtmax                                         STRING    ,
          v1_raacrtrecbkrptmmbrcnt36mo                                       STRING    ,
          v1_raaoccproflicmmbrcnt                                            STRING    ,
          v1_raaoccbusinessassocmmbrcnt                                      STRING    ,
          v1_raainterestsportpersonmmbrcnt                                   STRING    ,
          arc_gender                                                         STRING    ,
          arc_age                                                            STRING    ,
          arc_dob                                                            STRING    ,
          arc_hhnbr                                                          STRING    ,
          arc_tot_males                                                      STRING    ,
          arc_tot_females                                                    STRING    ,
          arc_time_zone                                                      STRING    ,
          arc_daylight_savings                                               STRING    ,
          arc_telephonenumber_1                                              STRING    ,
          arc_dma_tps_dnc_flag_1                                             STRING    ,
          arc_telephonenumber_2                                              STRING    ,
          arc_dma_tps_dnc_flag_2                                             STRING    ,
          arc_telephonenumber_3                                              STRING    ,
          arc_dma_tps_dnc_flag_3                                             STRING    ,
          arc_tot_phones                                                     STRING    ,
          arc_length_of_residence                                            STRING    ,
          arc_homeowner                                                      STRING    ,
          arc_estimatedincome                                                STRING    ,
          arc_dwelling_type                                                  STRING    ,
          arc_married                                                        STRING    ,
          arc_child                                                          STRING    ,
          arc_nbrchild                                                       STRING    ,
          arc_teencd                                                         STRING    ,
          arc_percent_range_english_speaking                                 STRING    ,
          arc_percent_range_spanish_speaking                                 STRING    ,
          arc_percent_range_asian_speaking                                   STRING    ,
          arc_mhv                                                            STRING    ,
          arc_mor                                                            STRING    ,
          arc_car                                                            STRING    ,
          arc_medschl                                                        STRING    ,
          arc_penetration_range_whitecollar                                  STRING    ,
          arc_penetration_range_bluecollar                                   STRING    ,
          arc_penetration_range_otheroccupation                              STRING    ,
          hhld_vehicle_cnt                                                   STRING    ,
          presence_of_crossover                                              STRING    ,
          presence_of_fullsizecar                                            STRING    ,
          presence_of_fullsizesuv                                            STRING    ,
          presence_of_fullsizetruck                                          STRING    ,
          presence_of_fullsizevan                                            STRING    ,
          presence_of_midsizecar                                             STRING    ,
          presence_of_midsizetruck                                           STRING    ,
          presence_of_minivan                                                STRING    ,
          presence_of_smallcar                                               STRING    ,
          presence_of_smallsuv                                               STRING    ,
          presence_of_smalltruck                                             STRING    ,
          presence_of_luxury                                                 STRING    ,
          presence_of_truck                                                  STRING    ,
          vehicle_vin_1                                                      STRING    ,
          vehicle_make_code_1                                                STRING    ,
          vehicle_make_desc_1                                                STRING    ,
          vehicle_model_code_1                                               STRING    ,
          vehicle_model_desc_1                                               STRING    ,
          vehicle_year_1                                                     STRING    ,
          vehicle_series_1                                                   STRING    ,
          vehicle_class_1                                                    STRING    ,
          vehicle_style_1                                                    STRING    ,
          vehicle_body_1                                                     STRING    ,
          vehicle_fuel_type_1                                                STRING    ,
          vehicle_mfg_code_1                                                 STRING    ,
          vehicle_mileagecd_1                                                STRING    ,
          vehicle_vin_2                                                      STRING    ,
          vehicle_make_code_2                                                STRING    ,
          vehicle_make_desc_2                                                STRING    ,
          vehicle_model_code_2                                               STRING    ,
          vehicle_model_desc_2                                               STRING    ,
          vehicle_year_2                                                     STRING    ,
          vehicle_series_2                                                   STRING    ,
          vehicle_class_2                                                    STRING    ,
          vehicle_style_2                                                    STRING    ,
          vehicle_body_2                                                     STRING    ,
          vehicle_fuel_type_2                                                STRING    ,
          vehicle_mfg_code_2                                                 STRING    ,
          vehicle_mileagecd_2                                                STRING    ,
          vehicle_vin_3                                                      STRING    ,
          vehicle_make_code_3                                                STRING    ,
          vehicle_make_desc_3                                                STRING    ,
          vehicle_model_code_3                                               STRING    ,
          vehicle_model_desc_3                                               STRING    ,
          vehicle_year_3                                                     STRING    ,
          vehicle_series_3                                                   STRING    ,
          vehicle_class_3                                                    STRING    ,
          vehicle_style_3                                                    STRING    ,
          vehicle_body_3                                                     STRING    ,
          vehicle_fuel_type_3                                                STRING    ,
          vehicle_mfg_code_3                                                 STRING    ,
          vehicle_mileagecd_3                                                STRING    ,
          vehicle_vin_4                                                      STRING    ,
          vehicle_make_code_4                                                STRING    ,
          vehicle_make_desc_4                                                STRING    ,
          vehicle_model_code_4                                               STRING    ,
          vehicle_model_desc_4                                               STRING    ,
          vehicle_year_4                                                     STRING    ,
          vehicle_series_4                                                   STRING    ,
          vehicle_class_4                                                    STRING    ,
          vehicle_style_4                                                    STRING    ,
          vehicle_body_4                                                     STRING    ,
          vehicle_fuel_type_4                                                STRING    ,
          vehicle_mfg_code_4                                                 STRING    ,
          vehicle_mileagecd_4                                                STRING    ,
          vehicle_vin_5                                                      STRING    ,
          vehicle_make_code_5                                                STRING    ,
          vehicle_make_desc_5                                                STRING    ,
          vehicle_model_code_5                                               STRING    ,
          vehicle_model_desc_5                                               STRING    ,
          vehicle_year_5                                                     STRING    ,
          vehicle_series_5                                                   STRING    ,
          vehicle_class_5                                                    STRING    ,
          vehicle_style_5                                                    STRING    ,
          vehicle_body_5                                                     STRING    ,
          vehicle_fuel_type_5                                                STRING    ,
          vehicle_mfg_code_5                                                 STRING    ,
          vehicle_mileagecd_5                                                STRING    ,
          hhld_boat_cnt                                                      STRING    ,
          boat_make_1                                                        STRING    ,
          boat_model_1                                                       STRING    ,
          boat_year_1                                                        STRING    ,
          boat_length_1                                                      STRING    ,
          boat_size_1                                                        STRING    ,
          boat_type_1                                                        STRING    ,
          boat_use_1                                                         STRING    ,
          boat_fuel_1                                                        STRING    ,
          boat_hull_material_1                                               STRING    ,
          boat_hull_shape_1                                                  STRING    ,
          boat_propulsion_1                                                  STRING    ,
          boat_make_2                                                        STRING    ,
          boat_model_2                                                       STRING    ,
          boat_year_2                                                        STRING    ,
          boat_length_2                                                      STRING    ,
          boat_size_2                                                        STRING    ,
          boat_type_2                                                        STRING    ,
          boat_use_2                                                         STRING    ,
          boat_fuel_2                                                        STRING    ,
          boat_hull_material_2                                               STRING    ,
          boat_hull_shape_2                                                  STRING    ,
          boat_propulsion_2                                                  STRING    ,
          hhld_motorcycle_cnt                                                STRING    ,
          motorcycle_vin_1                                                   STRING    ,
          motorcycle_make_1                                                  STRING    ,
          motorcycle_model_1                                                 STRING    ,
          motorcycle_year_1                                                  STRING    ,
          motorcycle_series_1                                                STRING    ,
          motorcycle_class_1                                                 STRING    ,
          motorcycle_style_1                                                 STRING    ,
          motorcycle_vehicle_type_1                                          STRING    ,
          motorcycle_fuel_type_1                                             STRING    ,
          motorcycle_mfg_code_1                                              STRING    ,
          motorcycle_mileagecd_1                                             STRING    ,
          motorcycle_vin_2                                                   STRING    ,
          motorcycle_make_2                                                  STRING    ,
          motorcycle_model_2                                                 STRING    ,
          motorcycle_year_2                                                  STRING    ,
          motorcycle_series_2                                                STRING    ,
          motorcycle_class_2                                                 STRING    ,
          motorcycle_style_2                                                 STRING    ,
          motorcycle_vehicle_type_2                                          STRING    ,
          motorcycle_fuel_type_2                                             STRING    ,
          motorcycle_mfg_code_2                                              STRING    ,
          motorcycle_mileagecd_2                                             STRING    ,
          MES_ENHANCED                                                       STRING    ,
          MES_CONFIDENCE                                                     STRING    ,
          NIXIE                                                              STRING    ,
          truetouch_savvyresearchers                                         STRING    ,
          truetouch_organicnatural                                           STRING    ,
          truetouch_brandloyalists                                           STRING    ,
          truetouch_trendsetters                                             STRING    ,
          truetouch_dealseekers                                              STRING    ,
          truetouch_recreationalshoppers                                     STRING    ,
          truetouch_qualitymatters                                           STRING    ,
          truetouch_inthemomentshoppers                                      STRING    ,
          truetouch_mainstreamadopters                                       STRING    ,
          truetouch_noveltyseekers                                           STRING    ,
          truetouchconv_onlinedealvoucher                                    STRING    ,
          truetouchconv_discountsupercenters                                 STRING    ,
          truetouchconv_ebidsites                                            STRING    ,
          truetouchconv_etailonly                                            STRING    ,
          truetouchconv_midhighendstore                                      STRING    ,
          truetouchconv_specialtydeptstore                                   STRING    ,
          truetouchconv_specialtyboutique                                    STRING    ,
          truetouchconv_wholesale                                            STRING    ,
          truetoucheng_broadcastcabletv                                      STRING    ,
          truetoucheng_digitaldisplay                                        STRING    ,
          truetoucheng_digitalnewspaper                                      STRING    ,
          truetoucheng_digitalvideo                                          STRING    ,
          truetoucheng_directmail                                            STRING    ,
          truetoucheng_mobilesmsmms                                          STRING    ,
          truetoucheng_radio                                                 STRING    ,
          truetoucheng_streamingtv                                           STRING    ,
          truetoucheng_traditionalnewspaper                                  STRING    ,
          adw_lake_insert_datetime                                           datetime  ,
          adw_lake_insert_batch_number                                       int64
    );"""

    task = int_u.run_query('adw_build_market_magnifier', sql)

    return task


########
# UDFS #
########

def bq_create_udf_standardize_string():
    standardize_string = f"""
    create or replace function `{int_u.INTEGRATION_PROJECT}.udfs.standardize_string`(s STRING) AS ((
    select lower(trim(coalesce(s, '')))
    ))
    """

    task = int_u.run_query('create_standardize_udf', standardize_string)

    return task


def bq_create_udf_format_phone_number():
    format_phone_number = f"""CREATE OR REPLACE FUNCTION `{int_u.INTEGRATION_PROJECT}.udfs.format_phone_number`(s STRING) AS ((
    (
    select case when CHARACTER_LENGTH(s) = 7 then concat(SUBSTR(s, 0,3), " - ", SUBSTR(s, 3, 4))
                when CHARACTER_LENGTH(s) = 10 then concat("(", SUBSTR(s, 0,3), ") - ", SUBSTR(s,4, 3), " - ", SUBSTR(s, 7,4)) 
                when CHARACTER_LENGTH(s) = 11 then concat(SUBSTR(s, 0,1), " - ", "(", SUBSTR(s, 1,3), ") - ", SUBSTR(s,5, 3), " - ", SUBSTR(s, 8,4))
    else "" end
    )
    ));"""

    task = int_u.run_query('create_format_phone_number_udf', format_phone_number)

    return task


def bq_create_udf_parse_member_id():
    parse_member_id = f"""
    CREATE OR REPLACE FUNCTION `{int_u.INTEGRATION_PROJECT}.udfs.parse_member_id`(mbr_id STRING, type STRING, calr_mbr_clb_cd STRING, assoc_mbr_id STRING) AS ((
    SELECT
    CASE     WHEN type='iso_cd' THEN 
             if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
             ( CASE
                 WHEN LENGTH(TRIM(mbr_id))=16 THEN SUBSTR(mbr_id, 1, 3)
                 WHEN LENGTH(TRIM(mbr_id))=7 THEN CAST(NULL AS STRING)
                 WHEN LENGTH(TRIM(mbr_id))=9 THEN CAST(NULL AS STRING)
             END
             ))
             WHEN type='club_cd' THEN 
             if(SAFE_CAST(mbr_id AS FLOAT64) is null,null, 
             (CASE
                when length(trim(mbr_id))=16 then  SUBSTR(mbr_id, 4, 3) 
                when length(trim(mbr_id))=7 then  calr_mbr_clb_cd 
                when length(trim(mbr_id))=9 then  calr_mbr_clb_cd
            END
            ))
            WHEN type='membership_id' THEN 
            if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
            (CASE
                when length(trim(mbr_id))=16 then SUBSTR(mbr_id, 7, 7) 
                when length(trim(mbr_id))=7 then mbr_id 
                when length(trim(mbr_id))=9 then SUBSTR(mbr_id, 1, 7)
            END
            ))
            WHEN type='associate_id' THEN
            if(SAFE_CAST(mbr_id AS FLOAT64) is null,null, 
            (CASE
                when length(trim(mbr_id))=16 then SUBSTR(mbr_id, 14, 2) 
                when length(trim(mbr_id))=7 then assoc_mbr_id
                when length(trim(mbr_id))=9 then SUBSTR(mbr_id, 8, 2)
            END
            ))
            WHEN type='check_digit_nr' THEN 
            if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
            (CASE
                when  length(trim(mbr_id))=16 then SUBSTR(mbr_id, 16, 1)
                when length(trim(mbr_id))=7 then cast(null  as STRING) 
                when length(trim(mbr_id))=9 then cast(null  as STRING)
            END
            ))

        END
        AS type_ret
        ));"""

    task = int_u.run_query('create_member_id_udf', parse_member_id)

    return task


def bq_create_udf_parse_zipcode():
    parse_zipcode = f"""CREATE OR REPLACE FUNCTION `{int_u.INTEGRATION_PROJECT}.udfs.parse_zipcode`(s STRING, type STRING) AS ((
    (

    case when lower(type) = 'prefix' then
      case when CHARACTER_LENGTH(s) = 9 then SUBSTR(s, 0, 5)
      else s end
    when lower(type) = 'suffix' then
      case when CHARACTER_LENGTH(s) = 9 then SUBSTR(s,6,9)
        else null end
     else null end
    )
    ));"""

    task = int_u.run_query('create_parse_zipcode_udf', parse_zipcode)

    return task


def bq_create_udf_standardize_string():
    standardize_string = f"""
    create or replace function `{int_u.INTEGRATION_PROJECT}.udfs.standardize_string`(s STRING) AS ((
    select lower(trim(coalesce(s, '')))
    ))
    """

    task = int_u.run_query('create_standardize_udf', standardize_string)

    return task






with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:

    bq_create_udf_numeric_suffix_to_alpha = BigQueryOperator(
        task_id='numeric_suffix_to_alpha',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/udf/bq_udf_numeric_suffix_to_alpha.sql',
        use_legacy_sql=False
        )
    bq_udf_remove_selected_punctuation = BigQueryOperator(
        task_id='remove_selected_punctuation',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/udf/bq_udf_remove_selected_punctuation.sql',
        use_legacy_sql=False
    )
    bq_udf_diacritic_to_standard = BigQueryOperator(
        task_id='diacritic_to_standard',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/udf/bq_udf_diacritic_to_standard.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_address = BigQueryOperator(
        task_id='hash_address',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/udf/bq_udf_hash_address.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_email = BigQueryOperator(
        task_id='hash_email',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/udf/bq_udf_hash_email.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_name = BigQueryOperator(
        task_id='hash_name',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/udf/bq_udf_hash_name.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_phone = BigQueryOperator(
        task_id='hash_phone',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/udf/bq_udf_hash_phone.sql',
        use_legacy_sql=False
    )

    ########
    # Populate Stubbed records #
    ########

    bq_populate_dim_business_line = BigQueryOperator(
        task_id='hash_phone',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/stubbed_data/populate_dim_business_line.sql',
        use_legacy_sql=False
    )

    bq_populate_dim_vendor = BigQueryOperator(
        task_id='hash_phone',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/stubbed_data/populate_dim_vendor.sql',
        use_legacy_sql=False
    )

    bq_populate_dim_product_category = BigQueryOperator(
        task_id='hash_phone',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/stubbed_data/populate_dim_product_category.sql',
        use_legacy_sql=False
    )

    bq_db_cleanup = BigQueryOperator(
        task_id='bq_db_env_cleanup',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='sql/dbcleanup/bq_Integration_dbcleanup.sql',
        use_legacy_sql=False
    )

    # Build ingestion Datasets
    make_dataset_udfs = bq_make_dataset('udfs', f'{int_u.INTEGRATION_PROJECT}')
    make_dataset_adw_work = bq_make_dataset('adw_work', f'{int_u.INTEGRATION_PROJECT}')
    make_dataset_adw = bq_make_dataset('adw', f'{int_u.INTEGRATION_PROJECT}')
    make_dataset_adw_pii = bq_make_dataset('adw_pii', f'{int_u.INTEGRATION_PROJECT}')
    make_dataset_insurance = bq_make_dataset('insurance', f'{int_u.INTEGRATION_PROJECT}')
    make_dataset_member = bq_make_dataset('member', f'{int_u.INTEGRATION_PROJECT}')

    make_dataset_ingest_adw = bq_make_dataset('adw', f'{iu.INGESTION_PROJECT}')

    # Existing DB Env. cleanup
    bq_db_cleanup >> [make_dataset_udfs, make_dataset_adw_pii, make_dataset_adw, make_dataset_insurance,make_dataset_member,make_dataset_adw_work]

    # Build udfs in Dataset udfs
    make_dataset_udfs >> bq_create_udf_standardize_string()
    make_dataset_udfs >> bq_create_udf_format_phone_number()
    make_dataset_udfs >> bq_create_udf_parse_member_id()
    make_dataset_udfs >> bq_create_udf_parse_zipcode()

    make_dataset_udfs >> bq_create_udf_numeric_suffix_to_alpha
    make_dataset_udfs >> bq_udf_remove_selected_punctuation
    make_dataset_udfs >> bq_udf_diacritic_to_standard
    make_dataset_udfs >> bq_udf_hash_address
    make_dataset_udfs >> bq_udf_hash_email
    make_dataset_udfs >> bq_udf_hash_name
    make_dataset_udfs >> bq_udf_hash_phone

    # Build ingestion tables
    make_dataset_adw_pii >> bq_adw_pii_dim_address()
    make_dataset_adw_pii >> bq_adw_pii_dim_contact_info()
    make_dataset_adw_pii >> bq_adw_pii_dim_email()
    make_dataset_adw_pii >> bq_adw_pii_dim_employee()
    make_dataset_adw_pii >> bq_adw_pii_dim_name()
    make_dataset_adw_pii >> bq_adw_pii_dim_phone()

    make_dataset_adw >> bq_adw_dim_aca_office()
    make_dataset_adw >> bq_adw_dim_business_line()
    make_dataset_adw >> bq_adw_dim_comment()
    make_dataset_adw >> bq_adw_dim_contact_source_key()
    make_dataset_adw >> bq_adw_dim_employee_role()
    make_dataset_adw >> bq_adw_dim_product()
    make_dataset_adw >> bq_adw_dim_product_category()
    make_dataset_adw >> bq_adw_dim_vendor()
    make_dataset_adw >> bq_adw_audit_contact_match()
    make_dataset_adw >> bq_adw_xref_contact_address()
    make_dataset_adw >> bq_adw_xref_contact_email()
    make_dataset_adw >> bq_adw_xref_contact_name()
    make_dataset_adw >> bq_adw_xref_contact_phone()
    make_dataset_adw >> bq_adw_build_market_magnifier()

    make_dataset_insurance >> bq_insurance_insurance_agency()
    make_dataset_insurance >> bq_insurance_insurance_agent()
    make_dataset_insurance >> bq_insurance_insurance_client()
    make_dataset_insurance >> bq_insurance_insurance_coverage()
    make_dataset_insurance >> bq_insurance_insurance_driver()
    make_dataset_insurance >> bq_insurance_insurance_line()
    make_dataset_insurance >> bq_insurance_insurance_monthly_policy_snapshot()
    make_dataset_insurance >> bq_insurance_insurance_policy()
    make_dataset_insurance >> bq_insurance_insurance_policy_transactions()
    make_dataset_insurance >> bq_insurance_insurance_quote()
    make_dataset_insurance >> bq_insurance_insurance_vehicle()

    make_dataset_member >> bq_member_dim_member()
    make_dataset_member >> bq_member_dim_member_auto_renewal_card()
    make_dataset_member >> bq_member_dim_member_rider()
    make_dataset_member >> bq_member_dim_membership()
    make_dataset_member >> bq_member_dim_membership_fee()
    make_dataset_member >> bq_member_dim_membership_marketing_segmentation()
    make_dataset_member >> bq_member_dim_membership_payment_plan()
    make_dataset_member >> bq_member_dim_membership_solicitation()
    make_dataset_member >> bq_member_membership_billing_detail()
    make_dataset_member >> bq_member_membership_billing_summary()
    make_dataset_member >> bq_member_membership_gl_payments_applied()
    make_dataset_member >> bq_member_membership_payment_applied_detail()
    make_dataset_member >> bq_member_membership_payment_applied_summary()
    make_dataset_member >> bq_member_membership_payments_received()
    make_dataset_member >> bq_member_sales_agent_activity()

    # Populate Stubbed Records

    bq_adw_dim_product_category() >> bq_udf_hash_address
    bq_adw_dim_business_line() >> bq_populate_dim_business_line
    bq_adw_dim_vendor() >> bq_populate_dim_vendor

