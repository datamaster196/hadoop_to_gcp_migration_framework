CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary`(
 mbrs_billing_summary_adw_key     String     NOT NULL,
    mbrs_adw_key                     String     NOT NULL,
    bill_summary_source_key          int64              ,
    bill_process_dt                  date             ,
    bill_notice_nbr                  int64              ,
    bill_amt                         Numeric   ,
    bill_credit_amt                  Numeric   ,
    bill_typ                         String      ,
    mbr_expiration_dt                date             ,
    bill_paid_amt                    Numeric   ,
    bill_renewal_method              String      ,
    bill_panel_cd                    String     ,
    bill_ebilling_ind               String      ,
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
