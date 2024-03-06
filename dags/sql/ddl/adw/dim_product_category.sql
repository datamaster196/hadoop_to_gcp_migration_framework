CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category`(
    product_category_adw_key         String   NOT NULL,
    biz_line_adw_key                 String   NOT NULL,
    product_category_cd              String   NOT NULL,
    product_category_desc            String   ,
    product_category_status_cd       String   ,
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