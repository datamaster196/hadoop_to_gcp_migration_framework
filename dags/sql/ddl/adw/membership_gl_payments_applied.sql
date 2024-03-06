CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied`(
    mbrs_gl_pymt_applied_adw_key      String     NOT NULL,
    mbrs_adw_key                      String     ,
    aca_office_adw_key                String     ,
    mbrs_payment_apd_summ_adw_key     String     ,
    gl_payment_journal_source_key     int64              ,
    gl_payment_gl_desc                String    ,
    gl_payment_account_nbr            String     ,
    gl_payment_post_dt                date             ,
    gl_payment_process_dt             date             ,
    gl_payment_journal_amt            Numeric   ,
    gl_payment_journal_created_dtm    datetime         ,
    gl_payment_journ_d_c_cd         String      ,
    effective_start_datetime          datetime         ,
    effective_end_datetime            datetime         NOT NULL,
    actv_ind                          String        NOT NULL,
    adw_row_hash                      String        NOT NULL,
    integrate_insert_datetime         datetime         NOT NULL,
    integrate_insert_batch_number     int64              NOT NULL,
    integrate_update_datetime         datetime         NOT NULL,
    integrate_update_batch_number        int64              NOT NULL
)
 ;

