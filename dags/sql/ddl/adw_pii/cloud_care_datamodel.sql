-- notification related tables 

CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_notification_data`(
id                                                         , 
service_name                      STRING           NOT NULL,
service_status					  STRING           NOT NULL,
service_description               STRING           NOT NULL,
pub_date                          datetime         NOT NULL,
outage_id                         INT64                    ,
event_date                        datetime         NOT NULL,
notification_type                 STRING           NOT NULL,
notif_insert_datetime             datetime         NOT NULL
)


CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_notification_config`(
id                                                 NOT NULL,
org_id					  STRING           NOT NULL,
service_name                      STRING           NOT NULL,
notification_type                          datetime         NOT NULL,
transaction_type                         INT64                    ,
notification_date                STRING           NOT NULL,
product_allignment            datetime         NOT NULL
csm_email
)


CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_notification_data`(
service_name                      STRING           NOT NULL,
service_status					  STRING           NOT NULL,
service_description               STRING           NOT NULL,
pub_date                          datetime         NOT NULL,
outage_id                         INT64                    ,
event_date                        datetime         NOT NULL,
notification_type                 STRING           NOT NULL,
notif_insert_datetime             datetime         NOT NULL
)
