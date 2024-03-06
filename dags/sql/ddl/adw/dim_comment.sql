CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_comment`(
    comment_adw_key                  String     NOT NULL,
    created_emp_adw_key              String     NOT NULL,
    resolved_emp_adw_key             String     NOT NULL,
    comment_relation_adw_key         String     NOT NULL,
    comment_source_system_nm         String     ,
    comment_source_system_key        int64              ,
    comment_creation_dtm             datetime         ,
    comment_text                     String   ,
    comment_resolved_dtm             datetime         ,
    comment_resltn_text              String   ,
    effective_start_datetime         datetime         NOT NULL,
    effective_end_datetime           datetime         NOT NULL,
    actv_ind                         String         NOT NULL,
    adw_row_hash                     String        ,
    integrate_insert_datetime        datetime         NOT NULL,
    integrate_insert_batch_number    int64              NOT NULL,
    integrate_update_datetime        datetime         NOT NULL,
    integrate_update_batch_number    int64              NOT NULL
)
;