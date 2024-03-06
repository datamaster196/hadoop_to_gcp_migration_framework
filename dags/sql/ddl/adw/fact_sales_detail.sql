CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`(
    fact_sales_detail_adw_key        String    NOT NULL Options(description='Unique key created for each Fact Sales Detail Record.'),
    sales_header_adw_key             String    NOT NULL Options(description='Unique key for each ACA User record'),
    product_adw_key                  String    NOT NULL Options(description='Unique key for the product record'),
    biz_line_adw_key                 String    NOT NULL Options(description='Unique key for the Business Line record'),
    line_item_nbr                    int64    Options(description='This is the ordinal position the record has on the Invoice')   ,
    item_cd                          String    Options(description='The item Code representing the item sold.')   ,
    item_desc                        String    Options(description='The Description of the Item Sold.')   ,
    item_category_cd                 String    Options(description='The category code to which the item belongs.  ')   ,
    item_category_desc               String    Options(description='The description to which the item belongs.  ')   ,
    quantity_sold                    numeric    Options(description='The number of this line item that was sold. This can be either a piece of physical merchandise, a service component or a unit of labor.  ')   ,
    unit_sale_Price                  numeric    Options(description='This was the price of the item as sold for each piece of merchandise.  ')   ,
    unit_labor_cost                  numeric    Options(description='The per unit cost of the labor component of a service provided to the customer.  The unit of measure can a flat rate or hourly component.  An example of a flat rate labor charge would be alignment while an hourly charge such as diagnostic time. ')   ,
    unit_item_cost                   numeric    Options(description='This is the cost of the physical item at the time of the sale. ')   ,
    extended_sale_price              numeric    Options(description='This is the unit sale price multiplied by the quantity sold.')   ,
    extended_labor_cost              numeric    Options(description='The Unit Labor Cost multiplied by the Quantity Sold.')   ,
    extended_item_cost               numeric    Options(description='This is the Unit Item Cost multiplied by the quantity sold.')  ,
    effective_start_datetime         datetime  NOT NULL Options(description='Date that this record is effectively starting'),
    effective_end_datetime           datetime  NOT NULL Options(description='Date that this record is effectively ending'),
    actv_ind                         String    NOT NULL Options(description='Indicates if the record is currently the active record'),
    adw_row_hash                     String    NOT NULL Options(description='Hash Key of appropriate fields to enable easier type 2 logic'),
    integrate_insert_datetime        datetime  NOT NULL Options(description='Datetime that this record was first inserted'),
    integrate_insert_batch_number    int64     NOT NULL Options(description='Batch that first inserted this record'),
    integrate_update_datetime        datetime  NOT NULL Options(description='Most recent datetime that this record was updated'),
    integrate_update_batch_number    int64     NOT NULL Options(description='Most recent batch that updated this record')
)
PARTITION BY _PARTITIONDATE
options(description="This table contains line item details associated with a single invoice header record.The natural key is the Sales Header ADW Key and the Line Item Number.")
;