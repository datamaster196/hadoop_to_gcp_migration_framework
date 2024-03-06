CREATE TABLE  IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout`(
    contact_pref_adw_key             STRING        NOT NULL,
    contact_adw_key                  STRING        NOT NULL,            
    topic_nm                         STRING                ,
    channel                          STRING                ,
    event_cd                         STRING                ,
    effective_start_datetime         DATETIME      NOT NULL,
    effective_end_datetime           DATETIME      NOT NULL,
    actv_ind                         STRING        NOT NULL,
    adw_row_hash                     STRING        NOT NULL,
    integrate_insert_datetime        DATETIME      NOT NULL,
    integrate_insert_batch_number    INT64         NOT NULL,
    integrate_update_datetime        DATETIME      NOT NULL,
    integrate_update_batch_number    INT64         NOT NULL
)
;