CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_transformed` AS
 SELECT
 business_line.biz_line_adw_key AS biz_line_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(vendor.address1,vendor.address2,'',vendor.city,vendor.cdstatecode,vendor.postalcode,'',vendor.cdcountrycode) as address_adw_key,
 '-1' as email_adw_key,
  vendor.vendor_cd AS vendor_cd,
 vendor.vendor_name AS vendor_name,
 vendor.vendor_typ AS vendor_typ,
 vendor.vendor_contact AS vendor_contact,
      vendor.preferred_vendor AS preferred_vendor,
      vendor.vendor_status AS vendor_status,
      CAST(vendor.updateddate AS datetime) AS effective_start_datetime,
vendor.address1,
vendor.address2,
vendor.city,
vendor.cdstatecode,
vendor.postalcode,
vendor.cdcountrycode,
      TO_BASE64(MD5(CONCAT(ifnull(CAST(business_line.biz_line_adw_key AS STRING),''),'|',
                           ifnull(CAST(vendor.vendor_name AS STRING),''),'|',
                           ifnull(CAST(vendor.vendor_typ AS STRING),''),'|',
                           ifnull(CAST(vendor.vendor_contact AS STRING),''),'|',
                           ifnull(CAST(vendor.preferred_vendor AS STRING),''),'|',
                           ifnull(CAST(vendor.vendor_status AS STRING),''),'|'
      ))) AS adw_row_hash
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_source` AS vendor,
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_business_line` business_line
        where business_line.biz_line_cd='INS'
        and vendor.dupe_check=1
    