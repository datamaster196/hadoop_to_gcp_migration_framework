CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email`(
    email_adw_key                    String       NOT NULL,
    raw_email_nm                     String   ,
    cleansed_email_nm                String   ,
    email_top_level_domain_nm        String     ,
    email_domain_nm                  String    ,
    email_valid_status_cd            String    ,
    email_valid_typ                  String    ,
    integrate_insert_datetime        datetime        NOT NULL,
    integrate_insert_batch_number    int64             NOT NULL,
    integrate_update_datetime        datetime        NOT NULL,
    integrate_update_batch_number    int64             NOT NULL
)
 ;