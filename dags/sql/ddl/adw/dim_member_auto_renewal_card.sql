CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card`(
    mbr_ar_card_adw_key              String   NOT NULL,
    nm_adw_key                       String      NOT NULL,
    address_adw_key                  String      NOT NULL,
    mbr_source_arc_key               int64            ,
    mbr_arc_status_dtm               datetime       ,
    mbr_cc_typ_cd                    String    ,
    mbr_cc_expiration_dt             date           ,
    mbr_cc_reject_reason_cd          String    ,
    mbr_cc_reject_dt                 date           ,
    mbr_cc_donor_nbr                 String    ,
    mbr_cc_last_four                 String    ,
    mbr_credit_debit_typ_cd          String    ,
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