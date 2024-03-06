CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_mbrs_marketing_segmntn`(
    mbrs_marketing_segmntn_adw_key    String    NOT NULL,
    mbrs_adw_key                      String    NOT NULL,
    mbr_expiration_dt                 date            ,
    segmntn_test_group_cd           String    ,
    segmntn_panel_cd                  String    ,
    segmntn_control_panel_ind        String     ,
    segmntn_comm_cd                   String    ,
    segment_nm                        String   ,
    effective_start_datetime          datetime        NOT NULL,
    effective_end_datetime            datetime        NOT NULL,
    actv_ind                          String        NOT NULL,
    adw_row_hash                      String       NOT NULL,
    integrate_insert_datetime         datetime        NOT NULL,
    integrate_insert_batch_number     int64             NOT NULL,
    integrate_update_datetime         datetime        NOT NULL,
    integrate_update_batch_number        int64             NOT NULL
)
 ;