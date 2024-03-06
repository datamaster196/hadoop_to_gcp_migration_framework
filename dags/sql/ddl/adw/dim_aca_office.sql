CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`(
    aca_office_adw_key               String    NOT NULL,
    aca_office_list_source_key       String       NOT NULL,
    aca_office_address               String   ,
    aca_office_state_cd              String    ,
    aca_office_phone_nbr             String   ,
    aca_office_nm                    String   ,
    aca_office_status                String     ,
    aca_office_typ                   String    ,
    mbrs_branch_cd                   String   ,
    ins_department_cd                String   ,
    rs_asstc_communicator_center     String   ,
    car_care_loc_nbr         String   ,
    tvl_branch_cd                    String    ,
    aca_office_division              String    ,
    retail_sales_office_cd              String   ,
    retail_manager                   String    ,
    retail_mngr_email                String    ,
    retail_assistant_manager         String   ,
    retail_assistant_mngr_email      String    ,
    staff_assistant                  String   ,
    staff_assistant_email            String    ,
    car_care_manager                 String   ,
    car_care_mngr_email              String    ,
    car_care_assistant_manager       String   ,
    car_care_assistant_mngr_email    String    ,
    region_tvl_sales_mgr            String   ,
    region_tvl_sales_mgr_email      String    ,
    retail_sales_coach               String   ,
    retail_sales_coach_email         String    ,
    xactly_retail_store_nbr          String    ,
    xactly_car_care_store_nbr        String    ,
    region                           String    ,
    car_care_general_manager         String   ,
    car_care_general_mngr_email      String    ,
    car_care_dirctr                  String   ,
    car_care_dirctr_email            String    ,
    car_care_district_nbr            String    ,
    car_care_cost_center_nbr         String    ,
    tire_cost_center_nbr             String    ,
    admin_cost_center_nbr            String    ,
    occupancy_cost_center_nbr        String    ,
    retail_general_manager           String   ,
    retail_general_mngr_email        String    ,
    district_manager                 String   ,
    district_mngr_email              String    ,
    retail_district_nbr              String    ,
    retail_cost_center_nbr           String    ,
    tvl_cost_center_nbr              String    ,
    mbrs_cost_center_nbr             String    ,
    facility_cost_center_nbr         String    ,
    national_ats_nbr                 String    ,
    mbrs_division_ky                 String    ,
    mbrs_division_nm                 String    ,
    effective_start_datetime         datetime        NOT NULL,
    effective_end_datetime           datetime        NOT NULL,
    actv_ind                         String        NOT NULL,
    adw_row_hash                     String       ,
    integrate_insert_datetime        datetime        NOT NULL,
    integrate_insert_batch_number    int64             NOT NULL,
    integrate_update_datetime        datetime        NOT NULL,
    integrate_update_batch_number    int64             NOT NULL
)
 ;