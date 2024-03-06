
CREATE TABLE IF NOT EXISTS  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan`(
    mbrs_payment_plan_adw_key         String    NOT NULL Options(description='(PK)  Unique value that is assigned to a payment plan for a member rider'),
    mbrs_adw_key                      String    NOT NULL Options(description='(FK)  Unique key for each Membership record'),
    mbr_rider_adw_key                 String    NOT NULL Options(description='(FK)  Unique value for each rider'),
    payment_plan_source_key           int64 Options(description='The source system key from which this record is coming from.  '),
    payment_plan_nm                   String Options(description='The name of the payment plan'),
    payment_plan_payment_cnt          int64 Options(description='The number of payments for the duration of the payment plan'),
    payment_plan_month                int64 Options(description='The duration in months over which the payment plan is spread'),
    payment_nbr                       int64 Options(description='The sequential number of the current payment record'),
    payment_plan_status               String Options(description='The status of the current payment plan for the membership'),
    payment_plan_charge_dt            date Options(description='The date the payment plan charge was applied'),
    safety_fund_donation_ind          String Options(description='This indicates if the payment record is for AAA National Safety Fund.  '),
    payment_amt                       numeric Options(description='The amount of the installment payment for the payment plan.'),                      
    effective_start_datetime          datetime  NOT NULL Options(description='Date that this record is effectively starting'),
    effective_end_datetime            datetime  NOT NULL Options(description='Date that this record is effectively ending'),
    actv_ind                          String    NOT NULL Options(description='Indicates if the record is currently the active record'),
    adw_row_hash                      String    NOT NULL Options(description='Hash Key of appropriate fields to enable easier type 2 logic'),
    integrate_insert_datetime         datetime  NOT NULL Options(description='Datetime that this record was first inserted'),
    integrate_insert_batch_number     int64     NOT NULL Options(description='Batch that first inserted this record'),
    integrate_update_datetime         datetime  NOT NULL Options(description='Most recent datetime that this record was updated'),
    integrate_update_batch_number     int64     NOT NULL Options(description='Most recent batch that updated this record')
)
options(description="This table sores the payment plan that is associated with each member rider where applicable")
 ;