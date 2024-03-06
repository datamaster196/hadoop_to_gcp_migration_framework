CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_segmentation`(
    contact_segmntn_adw_key        STRING   NOT NULL,
    contact_adw_key                STRING   NOT NULL,
    segment_source                 STRING   NOT NULL,
    segment_panel                  String   NOT NULL,
    segment_cd                     String ,
    segment_desc                   String ,
    actual_score_ind               String ,
    effective_start_datetime       datetime  NOT NULL,
    effective_end_datetime         datetime  NOT NULL,
    actv_ind                       String    NOT NULL,
    adw_row_hash                   String    NOT NULL,
    integrated_insert_datetime          datetime  NOT NULL,
    integrated_insert_batch_number    int64     NOT NULL,
    integrated_update_datetime          datetime  NOT NULL,
    integrated_update_batch_number    int64     NOT NULL
)
;
