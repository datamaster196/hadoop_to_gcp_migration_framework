-- Initial Cleanup: Drop any existing copy of work tables related to this table's load
Drop table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_fact_customer_source`;
Drop table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_fact_customer_work_transformed`;

-- Load source data in the work area table adw_work.mzp_fact_customer_source
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_fact_customer_source` AS
SELECT
  contact_adw_key
 ,mbr_status_cd
 ,mbrs_status_cd
 ,mbr_join_club_dt
 ,mbr_active_expiration_dt
 ,mbrs_out_of_territory_cd
 ,product_adw_key
 ,ROW_NUMBER() OVER(PARTITION BY contact_adw_key ORDER BY mbr_source_system_key DESC) AS DUPE_CHECK
FROM
   (
   select
       contact.contact_adw_key
      ,member.mbr_source_system_key
      ,member.mbr_status_cd
      ,membership.mbrs_status_cd
      ,member.mbr_join_club_dt
      ,member.mbr_active_expiration_dt
      ,membership.mbrs_out_of_territory_cd
      ,membership.product_adw_key
   from
      -- Start with All contacts --
      ( select contact_adw_key
         from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`
      ) contact
	  -- Filter: All contacts who are members --
      join
      ( select
           contact_adw_key
          ,mbr_source_system_key
          ,mbr_status_cd
          ,mbr_join_club_dt
          ,mbr_active_expiration_dt
          ,mbrs_adw_key
        from
          `adw-dev.adw.dim_member`
        where actv_ind = 'Y' and mbr_status_cd in ( 'A', 'P')
      ) member
	  on contact.contact_adw_key = member.contact_adw_key
	  -- Filter: All contacts who are members and have Active, Pending membership --
	  join
      ( select mbrs_adw_key
              ,mbrs_out_of_territory_cd
              ,product_adw_key
              ,mbrs_status_cd
	      from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`
	     where actv_ind = 'Y' and mbrs_status_cd in ( 'A', 'P')
	  ) membership
	  on member.mbrs_adw_key = membership.mbrs_adw_key
   ) contact_member_source
   ;