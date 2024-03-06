CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider`(
    mbr_rider_adw_key                String     NOT NULL,
    mbrs_adw_key                     String     NOT NULL,
    mbr_adw_key                      String     NOT NULL,
    mbr_ar_card_adw_key              String     NOT NULL,
    emp_role_adw_key                 String     NOT NULL,
    product_adw_key                  String     NOT NULL,
    mbr_rider_source_key             int64              ,
    mbr_rider_status_cd              String      ,
    mbr_rider_status_dtm             datetime         ,
    mbr_rider_cancel_dt              date             ,
    mbr_rider_cost_effective_dt      date             ,
    mbr_rider_solicit_cd             String     ,
    mbr_rider_do_not_renew_ind      String      ,
    mbr_rider_dues_cost_amt          Numeric   ,
    mbr_rider_dues_adj_amt           Numeric   ,
    mbr_rider_payment_amt            Numeric   ,
    mbr_rider_paid_by_cd             String      ,
    mbr_rider_future_cancel_dt       date             ,
    mbr_rider_billing_category_cd    String      ,
    mbr_rider_cancel_reason_cd       String      ,
    mbr_rider_cancel_reason_desc     string,
    mbr_rider_reinstate_ind         String      ,
    mbr_rider_effective_dt           date             ,
    mbr_rider_original_cost_amt      Numeric   ,
    mbr_rider_reinstate_reason_cd    String      ,
    mbr_rider_extend_exp_amt         Numeric   ,
    mbr_rider_actual_cancel_dtm      datetime         ,
    mbr_rider_def_key                int64              ,
    mbr_rider_actvtn_dt              date             ,
    effective_start_datetime         datetime         NOT NULL,
    effective_end_datetime           datetime         NOT NULL,
    actv_ind                         String         NOT NULL,
    adw_row_hash                     String        ,
    integrate_insert_datetime        datetime         NOT NULL,
    integrate_insert_batch_number    int64              NOT NULL,
    integrate_update_datetime        datetime         NOT NULL,
    integrate_update_batch_number    int64              NOT NULL
)
;