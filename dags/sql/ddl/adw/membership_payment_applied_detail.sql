CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`(
    mbrs_payment_apd_dtl_adw_key     String     NOT NULL,
    mbr_adw_key                      String     NOT NULL,
    mbrs_payment_apd_summ_adw_key    String     NOT NULL,
    product_adw_key                  String     NOT NULL,
    payment_apd_dtl_source_key       int64              ,
    payment_applied_dt               date             ,
    payment_method_cd                String      ,
    payment_method_desc              String     ,
    payment_detail_amt               Numeric   ,
    payment_applied_unapplied_amt    Numeric   ,
    discount_amt                     Numeric   ,
    pymt_apd_discount_counted        String      ,
    discount_effective_dt            date             ,
    rider_cost_effective_dtm         datetime         ,
    effective_start_datetime         datetime         NOT NULL,
    effective_end_datetime           datetime         NOT NULL,
    actv_ind                         String         NOT NULL,
    adw_row_hash                     String        NOT NULL,
    integrate_insert_datetime        datetime         NOT NULL,
    integrate_insert_batch_number    int64              NOT NULL,
    integrate_update_datetime        datetime         NOT NULL,
    integrate_update_batch_number    int64              NOT NULL
)
 ;