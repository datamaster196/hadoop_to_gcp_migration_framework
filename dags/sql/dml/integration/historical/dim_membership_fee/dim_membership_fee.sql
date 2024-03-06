INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee`
  (
mbrs_fee_adw_key,	
mbr_adw_key ,	
product_adw_key ,	
mbrs_fee_source_key ,	
status ,	
fee_typ,
fee_dt,
waived_dt,
waived_by,
donor_nbr,
waived_reason_cd,
effective_start_datetime,	
effective_end_datetime,	
actv_ind,
adw_row_hash ,
integrate_insert_datetime,	
integrate_insert_batch_number,	
integrate_update_datetime,	
integrate_update_batch_number
)
SELECT
mbrs_fee_adw_key ,	
source.mbr_adw_key ,	
source.product_adw_key ,	
source.mbrs_fee_source_key,	
source.status ,	
source.fee_typ,
source.fee_dt,
source.waived_dt,
source.waived_by ,
source.donor_nbr,
source.waived_reason_cd,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_transformed` source ON (
  source.mbrs_fee_source_key = hist_type_2.mbrs_fee_source_key
  AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbrs_fee_adw_key ,
    mbrs_fee_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_transformed`
  GROUP BY
    mbrs_fee_source_key ) pk
ON (source.mbrs_fee_source_key =pk.mbrs_fee_source_key);


  --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for mbr_adw_key

SELECT
     count(target.mbr_adw_key) AS mbr_adw_key_count
 FROM
     (select distinct mbr_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`)  target
       where not exists (select 1
                      from (select distinct mbr_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`) source_FK_1
                          where target.mbr_adw_key = source_FK_1.mbr_adw_key)
HAVING
 IF((mbr_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership_fee. FK Column: mbr_adw_key'));
 
  -- Orphaned foreign key check for  product_adw_key
 SELECT
     count(target.product_adw_key ) AS product_adw_key_count
 FROM
     (select distinct product_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`)  target
       where not exists (select 1
                      from (select distinct product_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) source_FK_2
                          where target.product_adw_key = source_FK_2.product_adw_key)
HAVING
 IF((product_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_membership_fee. FK Column: product_adw_key'));

----------------------------------------------------------------------------------------------
-- Duplicate Checks
-- Please alter the fields according to Adnan's duplicate key checks. Email Adnan if you don't have the queries.

 select count(1)
   from
   (select mbrs_fee_source_key , effective_start_datetime, count(*) as dupe_count
   from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`
   group by 1, 2
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_membership_fee'  ) );

 ---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a.mbrs_fee_adw_key ) from
 (select mbrs_fee_adw_key , mbrs_fee_source_key , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`) a
 join
 (select mbrs_fee_adw_key, mbrs_fee_source_key, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_fee`) b
 on a.mbrs_fee_adw_key=b.mbrs_fee_adw_key
    and a.mbrs_fee_source_key  = b.mbrs_fee_source_key
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.mbrs_fee_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_membership_fee' ));
