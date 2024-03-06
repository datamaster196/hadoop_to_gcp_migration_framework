CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_email`(
    contact_adw_key                  String      NOT NULL,
    contact_source_system_nm         String   NOT NULL,
    email_typ_cd                     String   NOT NULL,
    email_adw_key                    String      NOT NULL,
    effective_start_datetime         datetime       NOT NULL,
    effective_end_datetime           datetime     NOT NULL  ,
    actv_ind                         String       NOT NULL,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            NOT NULL,
    integrate_update_datetime        datetime       NOT NULL,
    integrate_update_batch_number    int64            NOT NULL
)
 ;