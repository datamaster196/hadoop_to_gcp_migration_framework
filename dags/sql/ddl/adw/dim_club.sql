CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_club`(
    club_adw_key                     String   NOT NULL,
    address_adw_key                  String      ,
    phone_adw_key                    String      ,
    club_cd                          String      ,
    club_nm                          String      ,
    club_iso_cd                      String      ,
    club_merchant_nbr                String      ,
    effective_start_datetime         datetime       ,
    effective_end_datetime           datetime       ,
    actv_ind                         String       ,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            ,
    integrate_update_datetime        datetime       NOT NULL,
    integrate_update_batch_number    int64          NOT NULL
)
;
