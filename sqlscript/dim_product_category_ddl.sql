CREATE TABLE adw.dim_product_category(
    product_category_adw_key         STRING    NOT NULL,
    business_line_adw_key            STRING    NOT NULL,
    product_category_code            STRING    NOT NULL,
    product_category_description     STRING,
    product_category_status_code     STRING,
    effective_start_datetime         DATETIME           NOT NULL,
    effective_end_datetime           DATETIME           NOT NULL,
    active_indicator                 STRING        NOT NULL,
    adw_row_hash                     STRING       NOT NULL,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL,
    integrate_update_datetime        DATETIME           NOT NULL,
    integrate_update_batch_number    INT64        NOT NULL
)
;
