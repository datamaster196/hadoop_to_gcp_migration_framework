insert into `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor`
(
vendor_adw_key
,biz_line_adw_key
,address_adw_key
,email_adw_key
,vendor_cd
,vendor_name
,vendor_typ
,internal_facility_cd
,vendor_contact
,preferred_vendor
,vendor_status
,service_region
,service_region_desc
,fleet_ind
,aaa_facility
,effective_start_datetime
,effective_end_datetime
,actv_ind
,adw_row_hash
,integrate_insert_datetime
,integrate_insert_batch_number
,integrate_update_datetime
,integrate_update_batch_number
)
select
a.vendor_adw_key,
a.biz_line_adw_key,
a.address_adw_key,
a.email_adw_key,
a.vendor_cd,
a.vendor_name,
a.vendor_typ,
a.internal_facility_cd,
a.vendor_contact,
a.preferred_vendor,
a.vendor_status,
a.service_region,
a.service_region_desc,
a.fleet_ind,
a.aaa_facility,
a.effective_start_datetime,
a.effective_end_datetime,
a.actv_ind,
a.adw_row_hash,
a.integrate_insert_datetime,
a.integrate_insert_batch_number,
a.integrate_update_datetime,
a.integrate_update_batch_number
from
(
select
'6e3dfbd4-2ce0-41a1-ade9-6407d1c2fe52' as vendor_adw_key
,'33b05324-9a68-4775-9c89-eee81867b23d'  as biz_line_adw_key
,'-1'  as address_adw_key
,'-1'  as email_adw_key
,'ACA'  as vendor_cd
,'AAA Club Alliance'  as vendor_name
,'Membership'  as vendor_typ
,null  as internal_facility_cd
,''  as vendor_contact
,''  as preferred_vendor
,'Active'  as vendor_status
,''  as service_region
,''  as service_region_desc
,'U' as fleet_ind
,''  as aaa_facility
,CAST('1900-01-01' as DATETIME) AS effective_start_datetime
,CAST('9999-12-31' as DATETIME) AS effective_end_datetime
,'Y'  as actv_ind
,''  as adw_row_hash
,CURRENT_DATETIME  as integrate_insert_datetime
,{{ dag_run.id }}  as integrate_insert_batch_number
,CURRENT_DATETIME  as integrate_update_datetime
,{{ dag_run.id }}  as integrate_update_batch_number
UNION ALL
SELECT
'-2' AS vendor_adw_key,
'49e503c9-8f79-4574-8391-68bfc6ca8f5d' AS biz_line_adw_key,
'-1' AS address_adw_key,
'-1' AS email_adw_key,
'ALL' AS vendor_cd,
'All Carriers' AS vendor_name,
'Insurance' AS vendor_typ,
null AS internal_facility_cd,
'' AS vendor_contact,
'' AS preferred_vendor,
'Active' AS vendor_status,
'' AS service_region,
'' AS service_region_desc,
'U' AS fleet_ind,
'' AS aaa_facility,
'1900-01-01' AS effective_start_datetime,
'9999-12-31' AS effective_end_datetime,
'Y' AS actv_ind,
'' AS adw_row_hash,
CURRENT_DATETIME AS integrate_insert_datetime,
{{ dag_run.id }} as integrate_insert_batch_number,
CURRENT_DATETIME AS integrate_update_datetime,
{{ dag_run.id }} as integrate_update_batch_number
    ) a
 where 0 = (select count(vendor_adw_key)
              from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor`);

