
CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_work_transformed` AS

  SELECT
      coalesce(member.mbr_adw_key,
        '-1') AS mbr_adw_key,
      coalesce(member.mbrs_adw_key,
        '-1') AS mbrs_adw_key,		
      coalesce(membership_solicitation.mbrs_solicit_adw_key, coalesce(membership_solicit_cd.mbrs_solicit_adw_key,
        '-1')) AS mbrs_solicit_adw_key,
        membership_solicitation.mbrs_solicit_adw_key AS original_mbrs_solicit_adw_key,
		membership_solicit_cd.mbrs_solicit_adw_key as code_mbrs_solicit_adw_key,
      coalesce(aca_office_sales.aca_office_adw_key,
        '-1') AS aca_sales_office,
      coalesce(aca_office_membership.aca_office_adw_key,
        '-1') AS aca_membership_office_adw_key,
      coalesce(employee_role.emp_role_adw_key,
        '-1') AS emp_role_adw_key,
      coalesce(product.product_adw_key,
        '-1') AS product_adw_key,
      coalesce(mbrs_payment_applied_detail.mbrs_payment_apd_dtl_adw_key,
        '-1') AS mbrs_payment_apd_dtl_adw_key,
      CASE
        WHEN sales_agent_activity.data_source = "SALES_AGENT_ACTIVITY" THEN "SAA"
        WHEN sales_agent_activity.data_source = "DAILY_COUNT_SUMMARY" THEN "DCS"
      ELSE
      NULL
    END
      AS saa_source_nm,
      safe_cast(sales_agent_activity.SALES_AGENT_ACTIVITY_KY as INT64) AS saa_source_key,
      safe_cast(safe_cast(sales_agent_activity.TRANSACTION_DT as DATETIME) as DATE) AS saa_tran_dt,
      sales_agent_activity.COMMISSION_CD AS mbr_comm_cd,
      commision_code.code_desc AS mbr_comm_desc,
      sales_agent_activity.RIDER_COMP_CD AS mbr_rider_cd,
      rider_code.code_desc AS mbr_rider_desc,
      safe_cast(safe_cast(sales_agent_activity.EXPIRATION_DT  as DATETIME) as DATE) AS mbr_expiration_dt,
      sales_agent_activity.SOLICITATION_CD AS rider_solicit_cd,
      sales_agent_activity.SOURCE_OF_SALE AS rider_source_of_sale_cd,
      sales_agent_activity.TRANSACTION_CD AS sales_agent_tran_cd,
      safe_cast(sales_agent_activity.DUES_AT as NUMERIC) AS saa_dues_amt,
      sales_agent_activity.DAILY_BILLED_IND AS saa_daily_billed_ind,
      sales_agent_activity.BILLING_CATEGORY_CD AS rider_billing_category_cd,
      billing_category_code.code_desc AS rider_billing_category_desc,
      sales_agent_activity.BILLING_SUBCAT_CD AS mbr_typ_cd,
      sales_agent_activity.COMMISSIONABLE_FL AS saa_commsable_activity_ind,
      sales_agent_activity.ADD_ON_FL AS saa_add_on_ind,
      sales_agent_activity.AUTORENEWAL_FL AS saa_ar_ind,
      sales_agent_activity.FEE_TYPE AS saa_fee_typ_cd,
      sales_agent_activity.DONATION_KY AS safety_fund_donation_ind ,
      sales_agent_activity.paymentplan_fl AS saa_payment_plan_ind,
      sales_agent_activity.ZIP AS zip_cd,
      sales_agent_activity.DAILY_COUNT_IND AS saa_daily_cnt_ind,
      safe_cast(sales_agent_activity.DAILY_COUNT as INT64) AS daily_cnt,
    CAST (sales_agent_activity.LAST_UPD_DT AS datetime) AS last_upd_dt,
      TO_BASE64(MD5(CONCAT(ifnull(coalesce(member.mbr_adw_key,
                '-1'),
              ''),'|',ifnull(coalesce(member.mbrs_adw_key,			  
                '-1'),
              ''),'|',ifnull(coalesce(membership_solicitation.mbrs_solicit_adw_key, coalesce(membership_solicit_cd.mbrs_solicit_adw_key,
        '-1')),
              ''),'|',ifnull(coalesce(aca_office_sales.aca_office_adw_key,
                '-1'),
              ''),'|',ifnull(coalesce(employee_role.emp_role_adw_key,
                '-1'),
              ''),'|',ifnull(coalesce(product.product_adw_key,
                '-1'),
              ''),'|',ifnull(coalesce(mbrs_payment_applied_detail.mbrs_payment_apd_dtl_adw_key,
                '-1'),
              ''),'|',ifnull(case
                WHEN sales_agent_activity.data_source = "SALES_AGENT_ACTIVITY" THEN "SAA"
                WHEN sales_agent_activity.data_source = "DAILY_COUNT_SUMMARY" THEN "DCS"
              ELSE
              NULL
            END
              ,
              ''),'|',ifnull(sales_agent_activity.SALES_AGENT_ACTIVITY_KY,
              ''),'|',ifnull(sales_agent_activity.TRANSACTION_DT,
              ''),'|',ifnull(sales_agent_activity.COMMISSION_CD,
              ''),'|',ifnull(commision_code.code_desc,
              ''),'|',ifnull(sales_agent_activity.RIDER_COMP_CD,
              ''),'|',ifnull(rider_code.code_desc,
              ''),'|',ifnull(sales_agent_activity.EXPIRATION_DT,
              ''),'|',ifnull(sales_agent_activity.SOLICITATION_CD,
              ''),'|',ifnull(sales_agent_activity.SOURCE_OF_SALE,
              ''),'|',ifnull(sales_agent_activity.TRANSACTION_CD,
              ''),'|',ifnull(sales_agent_activity.DUES_AT,
              ''),'|',ifnull(sales_agent_activity.DAILY_BILLED_IND,
              ''),'|',ifnull(sales_agent_activity.BILLING_CATEGORY_CD,
              ''),'|',ifnull(billing_category_code.code_desc,
              ''),'|',ifnull(sales_agent_activity.BILLING_SUBCAT_CD,
              ''),'|',ifnull(sales_agent_activity.COMMISSIONABLE_FL,
              ''),'|',ifnull(sales_agent_activity.ADD_ON_FL,
              ''),'|',ifnull(sales_agent_activity.AUTORENEWAL_FL,
              ''),'|',ifnull(sales_agent_activity.FEE_TYPE,
              ''),'|',ifnull(sales_agent_activity.DONATION_KY,
              ''),'|',ifnull(sales_agent_activity.paymentplan_fl,
              ''),'|',ifnull(sales_agent_activity.ZIP,
              ''),'|',ifnull(sales_agent_activity.DAILY_COUNT_IND,
              ''),'|',ifnull(sales_agent_activity.DAILY_COUNT,
              '') ))) AS adw_row_hash
  FROM
   `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_sales_agent_activity_source` AS sales_agent_activity

  LEFT JOIN (
    SELECT
      mbr_adw_key,
      mbrs_adw_key as mbrs_adw_key,
      mbr_source_system_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`
    WHERE
      actv_ind= 'Y' ) AS member
  ON
    safe_cast(sales_agent_activity.member_ky as INT64) = member.mbr_source_system_key

  LEFT JOIN
      (
         select solicitation_ky, 
				  SOURCE_OF_SALE,
				  SOLICITATION_CD,
        ROW_NUMBER() OVER (PARTITION BY SOURCE_OF_SALE, SOLICITATION_CD ORDER BY last_upd_dt desc) AS DUPE_CHECK
		     from `{{ var.value.INGESTION_PROJECT }}.mzp.solicitation` 
		) ings_solicitation
		on 
		   sales_agent_activity.SOURCE_OF_SALE = ings_solicitation.SOURCE_OF_SALE
		   and
		   sales_agent_activity.SOLICITATION_CD = ings_solicitation.SOLICITATION_CD				
     and ings_solicitation.DUPE_CHECK = 1
  LEFT JOIN
       ( select 
              mbrs_solicit_adw_key,
              mbrs_solicit_source_system_key
	         from
              `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` 
            WHERE
              actv_ind= 'Y' 
       ) AS membership_solicitation	   
         ON
           safe_cast(ings_solicitation.solicitation_ky as INT64) = membership_solicitation.mbrs_solicit_source_system_key

      LEFT JOIN
       ( select 
              mbrs_solicit_adw_key,
              solicit_cd,
              ROW_NUMBER() OVER (PARTITION BY solicit_cd ORDER BY mbrs_solicit_source_system_key desc) AS DUPE_CHECK
	         from
              `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` 
            WHERE
              actv_ind= 'Y' 
       ) AS membership_solicit_cd	   
         ON
           sales_agent_activity.solicitation_cd = membership_solicit_cd.solicit_cd and membership_solicit_cd.dupe_check = 1

      LEFT JOIN ( 
      (
          SELECT
            intg_aca_office.aca_office_adw_key,
            ingest_branch.branch_ky
          FROM (SELECT * FROM
          (
                 SELECT
                   aca_office_adw_key,
                   mbrs_branch_cd,
                   ROW_NUMBER() OVER (PARTITION BY mbrs_branch_cd ) AS DUPE_CHECK_OFFICE
                 FROM
                   `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`
                 WHERE
                   actv_ind= 'Y' 
			   )  WHERE DUPE_CHECK_OFFICE=1 ) intg_aca_office
          JOIN (
            SELECT
              branch_ky,
              branch_cd,
			ROW_NUMBER() OVER (PARTITION BY branch_cd ORDER BY last_upd_dt desc) AS DUPE_CHECK
            FROM
              `{{ var.value.INGESTION_PROJECT }}.mzp.branch` ) AS ingest_branch
          ON
            intg_aca_office.mbrs_branch_cd = ingest_branch.branch_cd and ingest_branch.DUPE_CHECK = 1)) AS aca_office_sales
        ON
          aca_office_sales.BRANCH_KY = sales_agent_activity.BRANCH_KY
      
        LEFT JOIN ( (
            SELECT
              intg_aca_office.aca_office_adw_key,
              ingest_branch.branch_ky
            FROM (SELECT * FROM
            (
              SELECT
                aca_office_adw_key,
                mbrs_branch_cd,
                ROW_NUMBER() OVER (PARTITION BY mbrs_branch_cd ) AS DUPE_CHECK_OFFICE
              FROM
                `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`
              WHERE
                actv_ind= 'Y') 
                WHERE DUPE_CHECK_OFFICE=1 ) intg_aca_office
            JOIN (
              SELECT
                branch_ky,
                branch_cd,
			    ROW_NUMBER() OVER (PARTITION BY branch_cd ORDER BY last_upd_dt desc) AS DUPE_CHECK
              FROM
                `{{ var.value.INGESTION_PROJECT }}.mzp.branch` ) AS ingest_branch
            ON
              intg_aca_office.mbrs_branch_cd = ingest_branch.branch_cd and ingest_branch.DUPE_CHECK = 1 )) AS aca_office_membership
          ON
            aca_office_membership.BRANCH_KY = sales_agent_activity.MEMBERSHIP_BRANCH_KY

        LEFT JOIN (
          SELECT
            emp_role_adw_key,
            emp_role_id
          FROM
            `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role`
          WHERE
            actv_ind= 'Y') AS employee_role
        ON
          sales_agent_activity.agent_id = employee_role.emp_role_id

        LEFT JOIN (

          SELECT
            a.product_adw_key,
            case when b.product_category_cd = 'MBR_FEES' then substr(a.product_sku_key,1,2) else a.product_sku_key end product_sku_key,
            a.product_category_adw_key
          FROM  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` a
          join `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` b
            on a.product_category_adw_key = b.product_category_adw_key
          WHERE
            a.actv_ind= 'Y'
            and b.actv_ind = 'Y'
            and b.product_category_cd in ( 'RCC', 'MBR_FEES') ) product
        ON
          sales_agent_activity.RIDER_COMP_CD = product.product_sku_key

       LEFT JOIN (
         SELECT 
           code,
           code_desc,
           ROW_NUMBER() OVER (PARTITION BY code ORDER BY last_upd_dt desc) AS DUPE_CHECK
         FROM
           `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
         WHERE
           code_type = 'COMMCD') AS commision_code
       ON
         upper(trim(sales_agent_activity.COMMISSION_CD)) = upper(trim(commision_code.code))
         and commision_code.DUPE_CHECK = 1

        LEFT JOIN (
          SELECT
            code,
            code_desc,
            ROW_NUMBER() OVER (PARTITION BY code ORDER BY last_upd_dt desc) AS DUPE_CHECK
          FROM
            `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
          WHERE
            code_type = 'RDTPCD') AS rider_code
        ON
          upper(trim(sales_agent_activity.RIDER_COMP_CD)) = upper(trim(rider_code.code))
          and rider_code.DUPE_CHECK = 1

       LEFT JOIN (
         SELECT 
           code,
           code_desc,
           ROW_NUMBER() OVER (PARTITION BY code ORDER BY last_upd_dt desc) AS DUPE_CHECK
         FROM
           `{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
         WHERE
           code_type = 'BILTYP') AS Billing_category_code

       ON
         upper(trim(sales_agent_activity.BILLING_CATEGORY_CD)) = upper(trim(Billing_category_code.code))
         and Billing_category_code.DUPE_CHECK = 1

        LEFT JOIN (
          SELECT
            mbrs_payment_apd_dtl_adw_key,
            mbr_adw_key,
            payment_apd_dtl_source_key
          FROM
            `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail`
          WHERE
            actv_ind= 'Y') AS mbrs_payment_applied_detail
        ON
          safe_cast(sales_agent_activity.PAYMENT_DETAIL_KY as INT64) = mbrs_payment_applied_detail.payment_apd_dtl_source_key
    
          WHERE
            sales_agent_activity.dupe_check=1


