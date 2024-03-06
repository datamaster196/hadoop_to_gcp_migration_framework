CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name`(
    nm_adw_key                       String       NOT NULL,
    raw_first_nm                     String   ,
    raw_middle_nm                    String   ,
    raw_last_nm                      String   ,
    raw_suffix_nm                    String     ,
    raw_title_nm                     String     ,
    cleansed_first_nm                String   ,
    cleansed_middle_nm               String   ,
    cleansed_last_nm                 String   ,
    cleansed_suffix_nm               String   ,
    cleansed_title_nm                String   ,
    nm_valid_status_cd               String    ,
    nm_valid_typ                     String    ,
    integrate_insert_datetime        datetime        NOT NULL,
    integrate_insert_batch_number    int64             NOT NULL,
    integrate_update_datetime        datetime        NOT NULL,
    integrate_update_batch_number    int64             NOT NULL
)
 ;