CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation`(
    mbrs_solicit_adw_key              String   NOT NULL,
    mbrs_solicit_source_system_key    int64            ,
    solicit_cd                        String   ,
    solicit_group_cd                  String    ,
    mbr_typ_cd                        String    ,
    solicit_camp_cd                   String    ,
    solicit_category_cd               String    ,
    solicit_discount_cd               String    ,
    effective_start_datetime          datetime       NOT NULL,
    effective_end_datetime            datetime       NOT NULL,
    actv_ind                          String       NOT NULL,
    adw_row_hash                      String      NOT NULL,
    integrate_insert_datetime         datetime       NOT NULL,
    integrate_insert_batch_number     int64            NOT NULL,
    integrate_update_datetime         datetime       NOT NULL,
    integrate_update_batch_number        int64            NOT NULL
)
 ;
