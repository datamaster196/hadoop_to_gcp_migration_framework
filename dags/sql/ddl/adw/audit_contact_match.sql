CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.audit_contact_match` (
    source_system_nm                 String   NOT NULL,
    source_system_key_1              String   NOT NULL,
    source_system_key_2              String   NOT NULL,
    source_system_key_3              String   NOT NULL,
    contact_adw_key                  String      NOT NULL,
    tgt_mch_on_source_key_ind       String       ,
    tgt_mch_on_mbr_id_ind           String       ,
    tgt_mch_on_address_nm_ind       String       ,
    tgt_mch_on_zip_nm_ind           String       ,
    tgt_mch_on_email_nm_ind         String       ,
    src_mch_on_mbr_id_ind           String       ,
    src_mch_on_address_nm_ind       String       ,
    src_mch_on_zip_nm_ind           String       ,
    src_mch_on_email_nm_ind         String       ,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            NOT NULL
)
