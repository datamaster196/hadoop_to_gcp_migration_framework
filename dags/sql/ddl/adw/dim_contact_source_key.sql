CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`(
    contact_adw_key                  String       NOT NULL,
    contact_source_system_nm         String    NOT NULL,
    key_typ_nm                       String    NOT NULL,
    source_1_key                     String   ,
    source_2_key                     String   ,
    source_3_key                     String   ,
    source_4_key                     String   ,
    source_5_key                     String   ,
    effective_start_datetime         datetime        NOT NULL,
    effective_end_datetime           datetime        NOT NULL,
    actv_ind                         String        NOT NULL,
    adw_row_hash                     String       NOT NULL,
    integrate_insert_datetime        datetime        NOT NULL,
    integrate_insert_batch_number    int64             NOT NULL,
    integrate_update_datetime        datetime        NOT NULL,
    integrate_update_batch_number    int64             NOT NULL
)
;
