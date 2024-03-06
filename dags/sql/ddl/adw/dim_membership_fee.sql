CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee`(
    mbrs_fee_adw_key                 String   NOT NULL,
    mbr_adw_key                      String   NOT NULL,
    product_adw_key                  String   NOT NULL,
    mbrs_fee_source_key              int64            ,
    status                           String    ,
    fee_typ                          String    ,
    fee_dt                           date           ,
    waived_dt                        date           ,
    waived_by                        int64            ,
    donor_nbr                        String   ,
    waived_reason_cd                 String    ,
    effective_start_datetime         datetime       NOT NULL,
    effective_end_datetime           datetime       NOT NULL,
    actv_ind                         String       NOT NULL,
    adw_row_hash                     String      NOT NULL,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            NOT NULL,
    integrate_update_datetime        datetime       NOT NULL,
    integrate_update_batch_number    int64            NOT NULL
)
 ;
