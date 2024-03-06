CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone`(
    phone_adw_key                    String      NOT NULL,
    raw_phone_nbr                    String   ,
    cleansed_ph_nbr                  String   ,
    cleansed_ph_area_cd_nbr          int64            ,
    cleansed_ph_prefix_nbr           int64            ,
    cleansed_ph_suffix_nbr           int64            ,
    cleansed_ph_extension_nbr        int64            ,
    phone_valid_status_cd            String   ,
    phone_valid_typ                  String   ,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            NOT NULL,
    integrate_update_datetime        datetime       NOT NULL,
    integrate_update_batch_number    int64            NOT NULL
)
 ;
