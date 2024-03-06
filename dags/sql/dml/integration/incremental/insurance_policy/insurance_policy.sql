MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_work_stage` b
ON
  (a. ins_policy_adw_key = b. ins_policy_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( ins_policy_adw_key, channel_adw_key, biz_line_adw_key, product_category_adw_key, ins_quote_adw_key, state_adw_key, emp_adw_key, ins_policy_system_source_key, ins_policy_quote_ind, ins_policy_number, ins_policy_effective_dt, ins_policy_Expiration_dt, ins_policy_cntrctd_exp_dt, ins_policy_annualized_comm, ins_policy_annualized_premium, ins_policy_billed_comm, ins_policy_billed_premium, ins_policy_estimated_comm, ins_policy_estimated_premium, effective_start_datetime, effective_end_datetime, actv_ind, adw_row_hash, integrate_insert_datetime, integrate_insert_batch_number, integrate_update_datetime, integrate_update_batch_number ) VALUES ( b.ins_policy_adw_key, b.channel_adw_key, b.biz_line_adw_key, b.product_category_adw_key, b.ins_quote_adw_key, b.state_adw_key, b.emp_adw_key, b.ins_policy_system_source_key, b.ins_policy_quote_ind, b.ins_policy_number, b.ins_policy_effective_dt, b.ins_policy_Expiration_dt, b.ins_policy_cntrctd_exp_dt, b.ins_policy_annualized_comm, b.ins_policy_annualized_premium, b.ins_policy_billed_comm, b.ins_policy_billed_premium, b.ins_policy_estimated_comm, b.ins_policy_estimated_premium, b.effective_start_datetime, b.effective_end_datetime, b.actv_ind, b.adw_row_hash, b.integrate_insert_datetime, b.integrate_insert_batch_number, b.integrate_update_datetime, b.integrate_update_batch_number )
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.actv_ind = b.actv_ind,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number;

------------------------------Audit Validation Queries---------------------------------------
---------------------------------------------------------------------------------------------
-- -- Orphaned foreign key check for insurance_policy
SELECT
        count(target.biz_line_adw_key ) AS biz_line_adw_key
FROM
        (select distinct biz_line_adw_key
        from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`)  target
        where not exists (select 1
                         from (select distinct biz_line_adw_key
                               from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_business_line`) source_FK_1
                               where target.biz_line_adw_key = source_FK_1.biz_line_adw_key)
HAVING
IF((biz_line_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy. FK Column: biz_line_adw_key'));


SELECT
       count(target.product_category_adw_key ) AS product_category_adw_key
FROM
         (select distinct product_category_adw_key
          from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`)  target
           where not exists (select 1
                          from (select distinct product_category_adw_key
                                from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category`) source_FK_1
                                where target.product_category_adw_key = source_FK_1.product_category_adw_key)
HAVING
IF((product_category_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy. FK Column: product_category_adw_key'));

SELECT
        count(target.state_adw_key ) AS state_adw_key
FROM
        (select distinct state_adw_key
         from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy` )  target
         where not exists (select 1
                         from (select distinct state_adw_key
                               from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_state`) source_FK_1
                               where target.state_adw_key = source_FK_1.state_adw_key)
HAVING
IF((state_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy. FK Column: state_adw_key'));


--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
      (select ins_policy_system_source_key, ins_policy_quote_ind , effective_start_datetime, count(*) as dupe_count
      from `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`
      group by 1, 2, 3
      having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.insurance_policy'  ) );

---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.ins_policy_system_source_key ) from
   (select ins_policy_system_source_key, ins_policy_quote_ind , effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`) a
join
   (select ins_policy_system_source_key, ins_policy_quote_ind, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`) b
on a.ins_policy_system_source_key=b.ins_policy_system_source_key
      and  a.ins_policy_quote_ind=b.ins_policy_quote_ind
      and a.effective_start_datetime  <= b.effective_end_datetime
      and b.effective_start_datetime  <= a.effective_end_datetime
      and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.ins_policy_system_source_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.insurance_policy' ));
