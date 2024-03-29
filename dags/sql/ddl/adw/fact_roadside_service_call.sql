CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_roadside_service_call`(
    rs_service_call_adw_key          String NOT NULL Options(description='Unique key for each Fact Service Call record.'),
    contact_adw_key                  String NOT NULL Options(description='The foreign key reference to the dim_contact_info table.'),
    product_adw_key                  String NOT NULL Options(description='This is the foreign Key reference to the product table representing the type of service such as Light Service, Tow Service, Locksmith etc.  '),
    office_commcenter_adw_key        String NOT NULL Options(description='The foreign key relationship to Dim ACA Office representing the Comm Center in which the service call is being handled.  '),
    svc_fac_vendor_adw_key           String NOT NULL Options(description='The foreign Key reference to Dim Vendor for the Service Facility servicing the breakdown vehicle.  '),
    ers_truck_adw_key                String NOT NULL Options(description='This is the foreign key for the service truck which serviced the breakdown vehicle.'),
    ers_drvr_adw_key                 String NOT NULL Options(description='This is the foreign key for the  service truck driver who serviced the breakdown vehicle.'),
    archive_dtm                      datetime Options(description='The date the service call was archived.'),
    archive_adw_key_dt               String Options(description='The foreign key reference to Dim Date for the Archive Date.'),
    service_call_dtm                 datetime NOT NULL Options(description='The date the service call was received.'),
    service_adw_key_dt               String Options(description='The foreign key reference to Dim Date for the service call was received.'),
    call_id                          int64 NOT NULL Options(description='The daily sequential number assigned by D3 to the service call. '),
    call_status_cd                   String Options(description='This is the codes for a call such as CL (Cleared normally), KI (The call is ended before performing service for the member)'),
    call_status_desc                 String Options(description='This table contains the Call Status Description for a call such as Clear (Cleared normally), Kill (The call is ended before performing service for the member)'),
    call_status_reason_cd            String Options(description='This is the reason code for why the call status we set.'),
    call_status_reason_desc          String Options(description='This is the description for why the call status we set.'),
    call_status_detail_reason_cd     String Options(description='This is the detailed reason code for why the call status we set.'),
    call_status_detail_reason_desc   String Options(description='This is the detailed description for why the call status we set.'),
    call_reassignment_cd             String Options(description='This is the reason code for why a Roadside Assistance call was reassigned to a new Service Truck. '),
    call_reassignment_desc           String Options(description='This is the reason description for why a Roadside Assistance call was reassigned to a new Service Truck. '),
    call_source_cd                   String Options(description='The code indicating how the call was initiated.  Examples are RAP, Reciprocals, Call Mover, Directly entered by a Call Rep etc.'),
    call_source_desc                 String Options(description='The description of how the call was initiated.  Examples are RAP, Reciprocals, Call Mover, Directly entered by a Call Rep etc.'),
    first_problem_cd                 String Options(description='The code indicating the initial Problem with the breakdown vehicle.'),
    first_problem_desc               String Options(description='The description indicating the initial Problem with the breakdown vehicle.'),
    final_problem_cd                 String Options(description='The code indicating the Problem with the breakdown vehicle which is resolved by the service truck driver'),
    final_problem_desc               String Options(description='The description indicating the Problem with the breakdown vehicle which is resolved by the service truck driver'),
    first_tlc_cd                     String Options(description='The Code representing the detailed problem code at the call is received.'),
    first_tlc_desc                   String Options(description='The Description representing the detailed problem code at the call is received.'),
    final_tlc_cd                     String Options(description='The Code representing the detailed problem code at the call is cleared.'),
    final_tlc_desc                   String Options(description='The Code representing the detailed problem code at the call is cleared.'),
    vehicle_year                     String Options(description='The year of the breakdown vehicle.'),
    vehicle_make                     String Options(description='The make of the breakdown breakdown vehicle.'),
    vehicle_model                    String Options(description='The model of the breakdown breakdown vehicle.'),
    promised_wait_tm                 int64 Options(description='The time in minutes given to the member for how long they will need to wait for the service truck to arrive.  '),
    loc_breakdown                    String Options(description='The location where the breakdown vehicle is located.'),
    tow_dest                         String Options(description='This is the location the breakdown vehicle was towed.'),
    total_call_cost                  String Options(description='The total cost of the call.'),
    fleet_Adjusted_cost              String Options(description='The adjusted cost of the call based on the actual ACA labor cost for an ACA Fleet call.'),
    effective_start_datetime         datetime  NOT NULL Options(description='Date that this record is effectively starting'),
    effective_end_datetime           datetime  NOT NULL Options(description='Date that this record is effectively ending'),
    actv_ind                         String    NOT NULL Options(description='Indicates if the record is currently the active record'),
    adw_row_hash                     String    NOT NULL Options(description='Hash Key of appropriate fields to enable easier type 2 logic'),
    integrate_insert_datetime        datetime  NOT NULL Options(description='Datetime that this record was first inserted'),
    integrate_insert_batch_number    int64     NOT NULL Options(description='Batch that first inserted this record'),
    integrate_update_datetime        datetime  NOT NULL Options(description='Most recent datetime that this record was updated'),
    integrate_update_batch_number    int64     NOT NULL Options(description='Most recent batch that updated this record')

)
PARTITION BY _PARTITIONDATE
options(description="This table contains attributes common to a Roadside Assistance Service Call.The natural key is the Comm Center, Service Call Date and Service Call Identifier")
;