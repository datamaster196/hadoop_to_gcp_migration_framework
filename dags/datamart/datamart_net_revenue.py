from datetime import datetime, timedelta, date
from dateutil.relativedelta import *
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

DAG_TITLE = 'datamart_net_revenue'
SCHEDULE = Variable.get("datamart_net_revenue_schedule", default_var='') or None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 19),
    'email': [FAIL_EMAIL],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360),
    'params': {
        'dag_name': DAG_TITLE
    }
}

ENV = Variable.get("environment").lower()
INGESTION_PROJECT = f'adw-lake-{ENV}'
INTEGRATION_PROJECT = f'adw-{ENV}'
DATAMART_PROJECT = f'adw-datamart-{ENV}'

# Month end to process or blank (default) to process previous month-end
netr_month_end = Variable.get("net_revenue_month_end", default_var='') or ''

def isValidateDate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False

if netr_month_end.strip() == '':
    wrk_dt = date(date.today().year, date.today().month, 1)
    month_end = wrk_dt + relativedelta(days=-1)
else:
    if isValidateDate(netr_month_end.strip()) == False:
        raise ValueError('Incorrect date format for variable "net_revenue_month_end", should be YYYY-MM-DD or a single space to calculate default value')
    month_end = datetime.strptime(netr_month_end.strip(), '%Y-%m-%d').date()

# Ensure that the date is a valid month end
wrk_dt = month_end + relativedelta(days=+1)
if wrk_dt.day != 1:
    wrk_dt = date(month_end.year, month_end.month, 1)
    wrk_dt = wrk_dt + relativedelta(months=+1)
    month_end = wrk_dt + relativedelta(days=-1)

if (month_end < (date.today() + relativedelta(years=-4)) or month_end > (date.today() + relativedelta(days=-1))):
    raise ValueError('Month End date is invalid')

period_end = month_end + relativedelta(days=+1)
period_start = period_end + relativedelta(years=-1)
prev_end = period_end + relativedelta(years=-1)
prev_start = prev_end + relativedelta(years=-1)
period_year_start = date(month_end.year, 1, 1)

run_values = f'Month End: {month_end};  Period #1: {prev_start} to  {prev_end};  Period #2: {period_start} to  {period_end};  Year Start: {period_year_start}'
print (run_values)

# Define SQL Queries
query_validation_check = f"""
    DECLARE month_end DATE DEFAULT DATE('{month_end}');
    IF (month_end > DATE('2018-03-01') AND NOT EXISTS(SELECT 1 FROM `{INGESTION_PROJECT}.misc.fleet_cpc` WHERE year = EXTRACT(YEAR FROM month_end) AND month = EXTRACT(MONTH FROM month_end))) THEN
        SELECT ERROR(CONCAT('Fleet CPC was not loaded for ', FORMAT_DATE('%B', month_end), ' ', FORMAT_DATE('%Y', month_end) ));
        RETURN;
    END IF;
"""

query_create_work_tables = f"""
  CREATE OR REPLACE TABLE `{DATAMART_PROJECT}.work.stg_netr_chrgers` (
    cc_tran_id      INT64       NOT NULL,
    membership_id   STRING      NOT NULL,
    associate_id    STRING      NOT NULL,
    yr_num          INT64       NOT NULL,
    ers_serv_date	DATE        NOT NULL,
    ers_call_id     INT64,
    pay_type        STRING,
    amount          NUMERIC		NOT NULL
  );

  CREATE OR REPLACE TABLE `{DATAMART_PROJECT}.work.stg_netr_dues` (
    id              STRING      NOT NULL,
    membership_id   STRING      NOT NULL,
    associate_id    STRING      NOT NULL,
    expiration_dt	DATE        NOT NULL,
    transaction_dt	DATE        NOT NULL,
    transaction_cd	STRING	    NOT NULL,
    dues_at			NUMERIC	    NOT NULL,
    yr_num          INT64	    NOT NULL,
    tot_months      INT64	    NOT NULL,
    earned_start    DATE		NOT NULL,
    earned_end      DATE		NOT NULL,
    dues_earned     NUMERIC,
    earned_mths     NUMERIC
  );

  CREATE OR REPLACE TABLE `{DATAMART_PROJECT}.work.stg_netr_dues_mth` (
    id              STRING      NOT NULL,
    month_earned    DATE		NOT NULL,
    dues_earned     NUMERIC	    NOT NULL
  );

  CREATE OR REPLACE TABLE `{DATAMART_PROJECT}.work.stg_netr_ers` (
    membership_id   STRING		NOT NULL,
    associate_id	STRING		NOT NULL,
    arch_date		DATETIME	NOT NULL,
    sc_dt           DATE        NOT NULL,
    yr_num          INT64		NOT NULL,
    call_cost       NUMERIC		NOT NULL,
    cc_amt          NUMERIC		NOT NULL,
    call_adj        NUMERIC		NOT NULL,
    facility_id     STRING,
    factor          NUMERIC,
    cpc             NUMERIC,
    fleet_call_cost	NUMERIC		NOT NULL,
    miles_towed		NUMERIC     NOT NULL,
    battery_replace_count INT64	NOT NULL
  );

  CREATE OR REPLACE TABLE `{DATAMART_PROJECT}.work.stg_netr_ersmbrbill` (
    primekey        STRING	    NOT NULL,
    membership_id	STRING		NOT NULL,
    associate_id    STRING		NOT NULL,
    yr_num          INT64       NOT NULL,
    pay_date        DATE        NOT NULL,
    sc_dt           DATE        NOT NULL,
    sc_id           INT64       NOT NULL,
    payment_type    STRING,
    paid_amount     NUMERIC		NOT NULL
  );

  CREATE OR REPLACE TABLE `{DATAMART_PROJECT}.work.stg_netr_inforce` (
    membership_id   STRING		NOT NULL,
    associate_id    STRING		NOT NULL
  );

  CREATE OR REPLACE TABLE `{DATAMART_PROJECT}.work.stg_netr_sumry` (
    membership_id   STRING		NOT NULL,
    associate_id    STRING		NOT NULL,
    lookback_period STRING		NOT NULL,
    earned_dues     NUMERIC		NOT NULL,
    dues_refund     NUMERIC		NOT NULL,
    call_count      INT64       NOT NULL,
    call_cost       NUMERIC		NOT NULL,
    call_adj        NUMERIC		NOT NULL,
    fees_paid       NUMERIC		NOT NULL,
    miles_towed		NUMERIC     NOT NULL,
    battery_replace_count INT64	NOT NULL
  );
"""

query_get_members_inforce = f"""
    DECLARE period_start DATE DEFAULT DATE('{period_start}');
    DECLARE period_end DATE DEFAULT DATE('{period_end}');

    DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_inforce` WHERE 1 = 1;

    INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_inforce` (
        membership_id, 
        associate_id
    )
    SELECT	membership_id, associate_id
    FROM
    (
        SELECT	mbrs_id as membership_id,
                mbr_assoc_id as associate_id,
                case when saa.sales_agent_tran_cd in ('CNLPRMBS', 'CNLASMBS', 'RVPPRMBS', 'RVPASMBS') then -1 
                    when saa.sales_agent_tran_cd in ('ADDPRMBS', 'ADDASMBS', 'RENPRMBS', 'RENASMBS', 'REIPRMBS', 'REIASMBS') then 1
                    else 0 
                end as InforceCnt
        FROM `{INTEGRATION_PROJECT}.adw.sales_agent_activity` saa 
        INNER JOIN `{INTEGRATION_PROJECT}.adw.dim_member` m on m.mbr_adw_key = saa.mbr_adw_key and m.actv_ind = 'Y'
        Where saa.actv_ind = 'Y' 
        and (saa.sales_agent_tran_cd LIKE 'ADD%' OR 
                saa.sales_agent_tran_cd LIKE 'REN%' OR 
                saa.sales_agent_tran_cd LIKE 'REI%' OR 
                saa.sales_agent_tran_cd LIKE 'RVP%'  OR 
                saa.sales_agent_tran_cd LIKE 'CNL%'
            )
        and SUBSTR(saa.sales_agent_tran_cd, 7, 2) = 'BS'
        and saa.saa_daily_cnt_ind = 'Y' 
        and saa.mbr_comm_cd <> 'T1' 
        and saa.saa_tran_dt >= period_start 
        and saa.saa_tran_dt < period_end
        and m.mbrs_id is not null
    ) a
    group by membership_id, associate_id
    having sum(InForceCnt) > 0;
"""

def query_calc_dues(yrnum, start_dt, end_dt):
    sql = f"""
    DECLARE yrnum INT64 DEFAULT {yrnum};
    DECLARE start_dt DATE DEFAULT DATE('{start_dt}');
    DECLARE end_dt DATE DEFAULT DATE('{end_dt}');

	DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_dues` WHERE yr_num = yrnum;

    -- Get from sales agent activity
	INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_dues` (
	    id, 
	    membership_id, 
	    associate_id, 
	    expiration_dt, 
	    transaction_dt, 
	    transaction_cd, 
	    dues_at, 
	    tot_months, 
	    yr_num, 
	    earned_start, 
	    earned_end
	)
	SELECT GENERATE_UUID() as id,
        m.mbrs_id as membership_id, 
        m.mbr_assoc_id as associate_id, 
        saa.mbr_expiration_dt as expiration_dt,
        saa.saa_tran_dt as transaction_dt,
        saa.sales_agent_tran_cd as transaction_cd,
        saa.saa_dues_amt as dues_at, 
        12 as tot_months,
        yrnum as yr_num,
        case when (saa.mbr_expiration_dt >= start_dt and saa.mbr_expiration_dt < end_dt ) then start_dt 
            when (saa.mbr_expiration_dt >= end_dt and saa.mbr_expiration_dt < DATE_ADD(end_dt, INTERVAL 1 YEAR)) then DATE_SUB(saa.mbr_expiration_dt, INTERVAL 1 YEAR)
            else NULL
        end as earned_start,
        case when (saa.mbr_expiration_dt >= start_dt and saa.mbr_expiration_dt < end_dt ) then saa.mbr_expiration_dt
            when (saa.mbr_expiration_dt >= end_dt and saa.mbr_expiration_dt < DATE_ADD(end_dt, INTERVAL 1 YEAR)) then end_dt
            else NULL
        end as earned_end
	FROM `{INTEGRATION_PROJECT}.adw.sales_agent_activity` AS saa
	INNER JOIN `{INTEGRATION_PROJECT}.adw.dim_member` AS m on m.mbr_adw_key = saa.mbr_adw_key and m.actv_ind = 'Y'
	WHERE (	(saa.sales_agent_tran_cd LIKE 'ADD%') OR
			(saa.sales_agent_tran_cd LIKE 'REN%') OR
			(saa.sales_agent_tran_cd LIKE 'REI%') OR
			(saa.sales_agent_tran_cd LIKE 'RVP%' and IFNULL(saa.saa_daily_cnt_ind,'Y') = 'Y' and saa.saa_dues_amt < 0) OR
			(saa.sales_agent_tran_cd LIKE 'CNL%' and IFNULL(saa.saa_daily_cnt_ind,'Y') = 'Y' and saa.saa_dues_amt < 0) 
		) 
    and saa.actv_ind = 'Y'
	and saa.mbr_rider_cd <> 'SF'
	and saa.saa_tran_dt > DATE_SUB(start_dt, INTERVAL 500 DAY)
	and saa.mbr_expiration_dt <> start_dt
	and (
		(saa.mbr_expiration_dt >= start_dt and saa.mbr_expiration_dt < end_dt ) or
		(saa.mbr_expiration_dt >= end_dt and saa.mbr_expiration_dt < DATE_ADD(end_dt, INTERVAL 1 YEAR) )
	)
	and m.mbrs_id IS NOT NULL
	and m.mbr_assoc_id IS NOT NULL;

	-- Calculate Dues collected for Period
	UPDATE `{DATAMART_PROJECT}.work.stg_netr_dues`
	SET    dues_earned = CAST(round( dues_at * ((DATE_DIFF(earned_end, earned_start, MONTH) * 1.0) / 12.0) ,2) AS NUMERIC),
	       earned_mths = DATE_DIFF(earned_end, earned_start, MONTH)
	WHERE  transaction_cd NOT like 'CNL%'
	AND    yr_num = yrnum;

	-- Calculate Refund for Period
    UPDATE `{DATAMART_PROJECT}.work.stg_netr_dues` AS a
	SET a.earned_start = refund_earned_start,
		a.tot_months = t.tot_months,
		a.dues_earned = CAST(case when t.tot_months = 0 then 0 else round(a.dues_at * ((DATE_DIFF(a.earned_end, t.refund_earned_start, MONTH) * 1.0) / (t.tot_months*1.0)), 2) end AS NUMERIC),
		a.earned_mths = DATE_DIFF(a.earned_end, t.refund_earned_start, MONTH)
	FROM (SELECT	ID,
			case when transaction_dt > earned_end then earned_end 
            when transaction_dt > earned_start then transaction_dt 
            else earned_start 
            end as refund_earned_start, 
            case when DATE_DIFF(expiration_dt, transaction_dt, MONTH) > 12 then 12 
                    when DATE_DIFF(expiration_dt, transaction_dt, MONTH) < 0 then 0 
                    else DATE_DIFF(expiration_dt, transaction_dt, MONTH) 
            end as tot_months
		FROM `{DATAMART_PROJECT}.work.stg_netr_dues`
		WHERE transaction_cd like 'CNL%'
		AND yr_num = yrnum
	) AS t 
    WHERE t.ID = a.ID;
    """
    return sql


def query_calc_ers(yrnum, start_dt, end_dt):
    sql = f"""
    DECLARE yrnum INT64 DEFAULT {yrnum};
    DECLARE start_dt DATE DEFAULT DATE('{start_dt}');
    DECLARE end_dt DATE DEFAULT DATE('{end_dt}');

    -- Service Calls (D3)
    DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_ers` WHERE yr_num = yrnum;
	INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_ers` (
	    membership_id, 
	    associate_id, 
	    arch_date, 
	    sc_dt, 
	    yr_num, 
	    call_cost, 
	    cc_amt, 
	    call_adj, 
	    facility_id, 
	    factor, 
	    CPC, 
	    fleet_call_cost, 
	    miles_towed, 
	    battery_replace_count
	)
    WITH tempERS AS
	(
		SELECT	a.sc_call_mbr_id as membership_id, 
				a.sc_call_asc_id as associate_id, 
				CAST(a.arch_date AS DATETIME) AS arch_date,
				DATE(CAST(a.sc_dt AS DATETIME)) AS sc_dt,
				yrnum as yr_num,
				IFNULL(cast(a.call_cost as NUMERIC),0.00) AS call_cost,
				CASE WHEN e.cc_chrg_flg ='Y' THEN IFNULL(CAST(e.cc_amt AS NUMERIC),0) ELSE 0.00 END AS cc_amt,
				CAST(IFNULL(d.adj_tot_cost,0) AS NUMERIC) AS call_adj,
				sf.svc_facl_id as facility_id,
				IFNULL(c.factor,0) as factor, 
				IFNULL(c.CPC ,0) as CPC,
				IFNULL(CAST(a.MILES_DEST as NUMERIC),0.0) AS miles_towed,
				CASE WHEN a.DTL_STS_RSN_CD IN ('G306', 'G317', 'G307', 'G308') THEN 1 ELSE 0 END AS battery_replace_count
		FROM (SELECT *, row_number() over(partition by comm_ctr_id, sc_dt, sc_id order by adw_lake_insert_datetime desc) as sequence FROM `{INGESTION_PROJECT}.d3.arch_call`) AS a
        LEFT JOIN (SELECT *, row_number() over(partition by comm_ctr_id, sc_dt, sc_id order by adw_lake_insert_datetime desc) as sequence 
               FROM `{INGESTION_PROJECT}.d3.arch_call_extd`) AS e ON a.comm_ctr_id = e.comm_ctr_id AND a.sc_id = e.sc_id AND a.sc_dt = e.sc_dt and e.sequence = 1	
		LEFT JOIN (SELECT comm_ctr_id, sc_dt, sc_id, SUM(IFNULL(CAST(adj_tot_cost as NUMERIC),0.00)) as adj_tot_cost
		         FROM (SELECT *, row_number() over(partition by comm_ctr_id, sc_dt, sc_id order by adw_lake_insert_datetime desc) as sequence FROM `{INGESTION_PROJECT}.d3.call_adj`) AS adj
		         WHERE del_flag = 'N' and sequence = 1
		         GROUP BY comm_ctr_id, sc_dt, sc_id
		) AS d on d.comm_ctr_id = a.comm_ctr_id and d.sc_dt = a.sc_dt and d.sc_id = a.sc_id
		LEFT JOIN (SELECT *, row_number() over(partition by facil_int_id order by adw_lake_insert_datetime desc) as sequence 
                FROM `{INGESTION_PROJECT}.d3.service_facility`) AS sf ON sf.FACIL_INT_ID = a.FACIL_INT_ID and sf.sequence = 1
		LEFT JOIN (SELECT *, row_number() over(partition by facilityid, year, month order by year desc) as sequence 
              FROM `{INGESTION_PROJECT}.misc.fleet_cpc`) AS c 
              ON c.facilityID = sf.svc_facl_id AND c.year = EXTRACT(YEAR FROM cast(a.arch_date as DATETIME)) AND c.month = EXTRACT(MONTH FROM cast(a.arch_date as DATETIME)) and c.sequence = 1
		WHERE   DATE(CAST(a.sc_dt as DATETIME)) >= start_dt 
		AND		DATE(CAST(a.sc_dt as DATETIME)) < end_dt
		AND		CAST(a.call_cost as NUMERIC) > 0 		
		AND		ifnull(a.status_cd,'') !='KI' 
		AND NOT (ifnull(a.call_source,'') = 'REM' and ifnull(a.chg_entitlement,'') = 'N') 
		AND		a.sc_call_clb_cd = '212'  
		AND		length(a.sc_call_mbr_id) = 7
		AND		length(a.sc_call_asc_id) = 2
        AND   a.sequence = 1
	) 
	SELECT  membership_id, 
	        associate_id, 
	        arch_date, 
	        sc_dt, 
	        yr_num, 
	        call_cost, 
	        cc_amt, 
	        call_adj, 
	        facility_id, 
	        factor, 
	        CPC, 
	        case when factor > 0 then ROUND(call_cost*factor,2) else 0.00 end as fleet_call_cost, 
	        miles_towed, 
	        battery_replace_count
	FROM tempERS;

    -- Charge ERS
	DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_chrgers` WHERE yr_num = yrnum;
    INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_chrgers` (
        cc_tran_id, 
        membership_id, 
        associate_id, 
        yr_num, 
        ers_serv_date, 
        ers_call_id, 
        pay_type, 
        amount
    )
    SELECT	cast(cc_tran_id as INT64) as cc_tran_id,
			substr(mbr_id,1,7) as membership_id,
			substr(mbr_id,8,2) as associate_id,
			yrnum as yr_num,
			DATE(CAST(ers_serv_date AS DATETIME)) as ers_serv_date,
			cast(ers_call_id as INT64) as ers_call_id,
			pay_type,
			cast(amount as NUMERIC) as amount
    FROM (SELECT *, row_number() over(partition by cc_tran_id order by adw_lake_insert_datetime desc) as sequence FROM `{INGESTION_PROJECT}.chargers.charg_cctran`)
	WHERE DATE(CAST(ers_serv_date as DATETIME)) >= start_dt 
	AND DATE(CAST(ers_serv_date as DATETIME)) < end_dt
	AND club_cd = '212'
	AND	TRAN_TYPE = 'C'
	AND	PAYX_RETURN_MSG = 'Success'
	AND	PAYX_AUTH_RESP_CODE ='A'
	AND sequence = 1;

    -- ERS Member Billing
	DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_ersmbrbill` WHERE yr_num = yrnum;
	INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_ersmbrbill` (
	    primekey, 
	    membership_id, 
	    associate_id, 
	    yr_num, 
	    pay_date, 
	    sc_dt, 
	    sc_id, 
	    payment_type, 
	    paid_amount
	)
    SELECT	primekey,
			SUBSTR(ltrim(sc_call_mbr_id),1,7) as membership_id,
			TRIM(sc_call_asc_id) as associate_id, 
			yrnum as yr_num,
			DATE(CAST(pay_date as DATETIME)) as pay_date,
			DATE(CAST(sc_dt as DATETIME)) as sc_dt, 
			CAST(sc_id as INT64) as sc_id,
			payment_type,
			case when paid_amount is null then 0 else CAST(paid_amount as NUMERIC) end as paid_amount
    FROM (SELECT *, row_number() over(partition by primekey order by adw_lake_insert_datetime desc) as sequence FROM `{INGESTION_PROJECT}.ersbilling.member_billing`)
	WHERE DATE(CAST(sc_dt as DATETIME)) >= start_dt 
	AND DATE(CAST(sc_dt as DATETIME)) < end_dt
	AND sc_call_clb_cd = '212'
	AND payment_type <> 'Credit Card'
	AND CAST(paid_amount as NUMERIC) > 0
    AND LENGTH(TRIM(sc_call_asc_id)) = 2
    AND sequence = 1;
    """
    return sql


query_gen_monthly_dues = f"""
    DECLARE period_year_start DATE DEFAULT DATE('{period_year_start}');
    DECLARE period_end DATE DEFAULT DATE('{period_end}');

	DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_dues_mth` WHERE 1 = 1;

	INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_dues_mth` (id, month_earned, dues_earned)
	SELECT id, earned_start, case when earned_mths = 0 then 0.00 else (dues_earned / earned_mths) end FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_start >= period_year_start AND earned_start < period_end 
	UNION ALL
	SELECT id, date_add(earned_start, INTERVAL 1 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 1 AND date_add(earned_start, INTERVAL 1 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 1 MONTH) < period_end 
	UNION ALL  
	SELECT id, date_add(earned_start, INTERVAL 2 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 2 AND date_add(earned_start, INTERVAL 2 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 2 MONTH) < period_end 
	UNION  ALL
	SELECT id, date_add(earned_start, INTERVAL 3 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 3 AND date_add(earned_start, INTERVAL 3 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 3 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 4 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 4 AND date_add(earned_start, INTERVAL 4 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 4 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 5 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 5 AND date_add(earned_start, INTERVAL 5 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 5 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 6 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 6 AND date_add(earned_start, INTERVAL 6 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 6 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 7 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 7 AND date_add(earned_start, INTERVAL 7 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 7 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 8 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 8 AND date_add(earned_start, INTERVAL 8 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 8 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 9 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 9 AND date_add(earned_start, INTERVAL 9 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 9 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 10 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 10 AND date_add(earned_start, INTERVAL 10 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 10 MONTH) < period_end 
	UNION ALL 
	SELECT id, date_add(earned_start, INTERVAL 11 MONTH), (dues_earned / earned_mths) FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
	WHERE earned_mths > 11 AND date_add(earned_start, INTERVAL 11 MONTH) >= period_year_start AND date_add(earned_start, INTERVAL 11 MONTH) < period_end;
"""

query_gen_summary_24mths = f"""
    DECLARE period_code STRING DEFAULT '24';

	DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_sumry` WHERE lookback_period = period_code;

	INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_sumry` (
	    membership_id, 
	    associate_id, 
	    lookback_period, 
	    earned_dues, 
	    dues_refund, 
	    call_count, 
	    call_cost, 
	    call_adj, 
	    fees_paid, 
	    miles_towed, 
	    battery_replace_count
	)
	SELECT	a.membership_id,
			a.associate_id,
			period_code,
			a.EarnedDues,
			a.DuesRefund,
			ifnull(b.CallCount,0) as CallCount,
			ifnull(b.CallCost,0.00) as CallCost,
			ifnull(b.CallAdj,0.00) as CallAdj,
			ifnull(b.FeesPaid,0.00) + ifnull(c.FeesPaid,0.00) as FeesPaid,
			ifnull(b.MilesTowed,0) as MilesTowed,
			ifnull(b.BatteryReplaceCount,0) as BatteryReplaceCount
	FROM (SELECT membership_id, 
				associate_id, 
			  	sum(case when (transaction_cd like 'CNL%') then 0 else dues_earned end) as EarnedDues,
				sum(case when (transaction_cd like 'CNL%') then abs(dues_earned) else 0 end) as DuesRefund
		 FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
		 GROUP BY membership_id, associate_id) a 
	LEFT JOIN 
		(SELECT membership_id,
				associate_id, 
				count(*) as CallCount,
				sum(case when fleet_call_cost > 0 then fleet_call_cost else call_cost end) as CallCost,
				sum(call_adj) as CallAdj,
				sum(cc_amt) as FeesPaid,
				sum(miles_towed) as MilesTowed,
				sum(battery_replace_count) as BatteryReplaceCount
		 FROM `{DATAMART_PROJECT}.work.stg_netr_ers`
		 GROUP BY membership_id, associate_id) b on b.membership_id = a.membership_id and b.associate_id = a.associate_id
	LEFT JOIN 
		(SELECT membership_id, associate_id, sum(amount) as FeesPaid
			FROM
			(SELECT	membership_id, associate_id, amount FROM `{DATAMART_PROJECT}.work.stg_netr_chrgers`
                UNION ALL
			 SELECT	membership_id,associate_id,	paid_amount FROM `{DATAMART_PROJECT}.work.stg_netr_ersmbrbill`
			) fees
		GROUP BY membership_id, associate_id) c  on c.membership_id = a.membership_id and c.associate_id = a.associate_id;
"""

query_gen_summary_12mths = f"""
    DECLARE period_code STRING DEFAULT '12';

	DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_sumry` WHERE lookback_period = period_code;

	INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_sumry` (
	    membership_id, 
	    associate_id, 
	    lookback_period, 
	    earned_dues, 
	    dues_refund, 
	    call_count, 
	    call_cost, 
	    call_adj, 
	    fees_paid, 
	    miles_towed, 
	    battery_replace_count
	)
	SELECT	a.membership_id,
		    a.associate_id,
		    period_code,
			a.EarnedDues,
			a.DuesRefund,
			ifnull(b.CallCount,0) as CallCount,
			ifnull(b.CallCost,0.00) as CallCost,
			ifnull(b.CallAdj,0.00) as CallAdj,
			ifnull(b.FeesPaid,0.00) + ifnull(c.FeesPaid,0) as FeesPaid,
			ifnull(b.MilesTowed,0) as MilesTowed,
			ifnull(b.BatteryReplaceCount,0) as BatteryReplaceCount
	FROM (SELECT membership_id, 
				associate_id, 
			  	sum(case when (transaction_cd like 'CNL%') then 0 else dues_earned end) as EarnedDues,
				sum(case when (transaction_cd like 'CNL%') then abs(dues_earned) else 0 end) as DuesRefund
		 FROM `{DATAMART_PROJECT}.work.stg_netr_dues` 
		 WHERE yr_num = 2
		 GROUP BY membership_id, associate_id) a 
	LEFT JOIN 
		(SELECT membership_id,
				associate_id, 
				count(*) as CallCount,
				sum(case when fleet_call_cost > 0 then fleet_call_cost else call_cost end) as CallCost,
				sum(call_adj) as CallAdj,
				sum(cc_amt) as FeesPaid,
				sum(miles_towed) as MilesTowed,
				sum(battery_replace_count) as BatteryReplaceCount
		 FROM `{DATAMART_PROJECT}.work.stg_netr_ers`
		 WHERE yr_num = 2
		 GROUP BY membership_id, associate_id) b on b.membership_id = a.membership_id and b.associate_id = a.associate_id
	LEFT JOIN 
		(SELECT membership_id, associate_id, sum(amount) as FeesPaid
		FROM
			(SELECT	membership_id, associate_id, amount FROM `{DATAMART_PROJECT}.work.stg_netr_chrgers` where yr_num = 2
			 UNION ALL
			 SELECT	membership_id,associate_id,	paid_amount FROM `{DATAMART_PROJECT}.work.stg_netr_ersmbrbill` where yr_num = 2
			) fees
		GROUP BY membership_id, associate_id) c  on c.membership_id = a.membership_id and c.associate_id = a.associate_id;
"""

query_gen_summary_ytd = f"""
	-- Load into stg_netr_sumry - YTD
    DECLARE period_year_start DATE DEFAULT DATE('{period_year_start}');
    DECLARE period_end DATE DEFAULT DATE('{period_end}');
    DECLARE period_code STRING DEFAULT 'YTD';

	DELETE FROM `{DATAMART_PROJECT}.work.stg_netr_sumry` WHERE lookback_period = period_code;

	INSERT INTO `{DATAMART_PROJECT}.work.stg_netr_sumry` (
	    membership_id, 
	    associate_id, 
	    lookback_period, 
	    earned_dues, 
	    dues_refund, 
	    call_count, 
	    call_cost, 
	    call_adj, 
	    fees_paid, 
	    miles_towed, 
	    battery_replace_count
	)
	SELECT	a.membership_id,
			a.associate_id,
			period_code,
			a.EarnedDues,
			a.DuesRefund,
			ifnull(b.CallCount,0) as CallCount,
			ifnull(b.CallCost,0.00) as CallCost,
			ifnull(b.CallAdj,0.00) as CallAdj,
			ifnull(b.FeesPaid,0.00) + ifnull(c.FeesPaid,0) as FeesPaid,
			ifnull(b.MilesTowed,0) as MilesTowed,
			ifnull(b.BatteryReplaceCount,0) as BatteryReplaceCount
	FROM (SELECT a2.membership_id, 
				a2.associate_id, 
			  	round(sum(case when (a2.transaction_cd like 'CNL%') then 0 else a1.dues_earned end),2) as EarnedDues,
				round(sum(case when (a2.transaction_cd like 'CNL%') then abs(a1.dues_earned) else 0 end),2) as DuesRefund
		 FROM `{DATAMART_PROJECT}.work.stg_netr_dues_mth` a1 
		 INNER JOIN `{DATAMART_PROJECT}.work.stg_netr_dues` a2 on a2.id = a1.id 
		 WHERE a1.month_earned >= period_year_start and a1.month_earned < period_end
		 GROUP BY a2.membership_id, a2.associate_id) a 
	LEFT JOIN 
		(SELECT membership_id,
				associate_id, 
				count(*) as CallCount,
				sum(case when fleet_call_cost > 0 then fleet_call_cost else call_cost end) as CallCost,
				sum(call_adj) as CallAdj,
				sum(cc_amt) as FeesPaid,
				sum(miles_towed) as MilesTowed,
				sum(battery_replace_count) as BatteryReplaceCount
		 FROM `{DATAMART_PROJECT}.work.stg_netr_ers`
		 WHERE sc_dt >= period_year_start and sc_dt < period_end
		 GROUP BY membership_id, associate_id) b on b.membership_id = a.membership_id and b.associate_id = a.associate_id
	LEFT JOIN 
		(SELECT membership_id, associate_id, sum(amount) as FeesPaid
		FROM
			(SELECT	membership_id, associate_id, amount FROM `{DATAMART_PROJECT}.work.stg_netr_chrgers` where ers_serv_date >= period_year_start and ers_serv_date < period_end
			 UNION ALL
			 SELECT	membership_id,associate_id,	paid_amount FROM `{DATAMART_PROJECT}.work.stg_netr_ersmbrbill` where sc_dt >= period_year_start and sc_dt < period_end
			) fees
		GROUP BY membership_id, associate_id) c  on c.membership_id = a.membership_id and c.associate_id = a.associate_id;
"""

query_load_net_revenue = f"""
    DECLARE month_end DATE DEFAULT DATE('{month_end}');

	DELETE `{DATAMART_PROJECT}.net_revenue.member_net_revenue` WHERE month_end_dt = month_end;
	INSERT INTO `{DATAMART_PROJECT}.net_revenue.member_net_revenue` (
	    month_end_dt, 
	    mbrs_id, 
	    mbr_assoc_id, 
	    mbr_adw_key, 
	    mbrs_adw_key, 
	    period_cd, 
	    period_desc, 
	    earned_dues, 
	    dues_refund, 
	    call_cnt, 
	    call_cost, 
	    call_adj, 
	    fees_paid, 
	    net_earned_dues, 
	    net_ers_cost, 
	    net_revenue, 
	    miles_towed, 
	    battery_replace_cnt, 
	    insert_datetime, 
	    insert_batch_number)
	SELECT	month_end, 
            a.membership_id, 
            a.associate_id, 
            m.mbr_adw_key,
            m.mbrs_adw_key,
            a.lookback_period as period_cd,
            case a.lookback_period when '12' then 'Rolling 12 months' when '24' then 'Rolling 24 months' when 'YTD' then 'Year to date' end as period_desc, 
            a.earned_dues, 
            a.dues_refund, 
            a.call_count, 
            a.call_cost, 
            a.call_adj, 
            a.fees_paid,
            case when a.dues_refund < a.earned_dues then a.earned_dues - a.dues_refund else 0.00 end as net_earned_dues,
            a.call_cost + a.call_adj - a.fees_paid as net_ers_cost, 
            (case when a.dues_refund < a.earned_dues then a.earned_dues - a.dues_refund else 0.00 end) - (a.call_cost + a.call_adj - a.fees_paid) as net_revenue,
            a.miles_towed,
            a.battery_replace_count,
            CURRENT_DATETIME() as insert_datetime,
            {{{{ dag_run.id }}}} as insert_batch_number
	FROM `{DATAMART_PROJECT}.work.stg_netr_sumry` a
	INNER JOIN `{INTEGRATION_PROJECT}.adw.dim_member` m on m.mbrs_id = a.membership_id and m.mbr_assoc_id = a.associate_id and m.actv_ind = 'Y';
"""

query_load_members_inforce = f"""
    DECLARE month_end DATE DEFAULT DATE('{month_end}');

	DELETE `{DATAMART_PROJECT}.net_revenue.member_inforce` WHERE month_end_dt = month_end;
	INSERT INTO `{DATAMART_PROJECT}.net_revenue.member_inforce` (
	    month_end_dt, 
	    mbrs_id, 
	    mbr_assoc_id, 
	    mbr_adw_key, 
	    mbrs_adw_key, 
	    insert_datetime, 
	    insert_batch_number)
	SELECT	month_end, 
            a.membership_id, 
            a.associate_id,
            m.mbr_adw_key,
            m.mbrs_adw_key,
            CURRENT_DATETIME() as insert_datetime,
            {{{{ dag_run.id }}}} as insert_batch_number
	FROM `{DATAMART_PROJECT}.work.stg_netr_inforce` a   
	INNER JOIN `{INTEGRATION_PROJECT}.adw.dim_member` m on m.mbrs_id = a.membership_id and m.mbr_assoc_id = a.associate_id and m.actv_ind = 'Y';
"""

def run_query(task_id, query):
    task = BigQueryOperator(task_id=task_id, sql=query, use_legacy_sql=False, retries=0)
    return task

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE, catchup=False, concurrency=6) as dag:
    # DAG Tasks

    task_validation_check = run_query('validation_check', query_validation_check)
    task_create_work_tables = run_query('create_work_tables', query_create_work_tables)

    task_get_members_inforce = run_query('get_members_inforce', query_get_members_inforce)
    task_load_members_inforce = run_query('load_members_inforce', query_load_members_inforce)

    task_calc_dues_year_1 = run_query('calc_dues_year_1', query_calc_dues(1, prev_start, prev_end))
    task_calc_dues_year_2 = run_query('calc_dues_year_2', query_calc_dues(2, period_start, period_end))
    task_gen_monthly_dues = run_query('gen_monthly_dues', query_gen_monthly_dues)

    task_calc_ers_year_1 = run_query('calc_ers_year_1', query_calc_ers(1, prev_start, prev_end))
    task_calc_ers_year_2 = run_query('calc_ers_year_2', query_calc_ers(2, period_start, period_end))

    task_gen_summary_24mths = run_query('gen_summary_24mths', query_gen_summary_24mths)
    task_gen_summary_12mths = run_query('gen_summary_12mths', query_gen_summary_12mths)
    task_gen_summary_ytd = run_query('gen_summary_ytd', query_gen_summary_ytd)

    task_load_net_revenue = run_query('load_net_revenue', query_load_net_revenue)

    # Task Sequence

    task_validation_check >> task_create_work_tables

    task_create_work_tables >> task_get_members_inforce >> task_load_members_inforce
    task_create_work_tables >> task_calc_dues_year_1 >> task_calc_dues_year_2 >> task_gen_monthly_dues >> task_gen_summary_24mths
    task_create_work_tables >> task_calc_ers_year_1 >> task_calc_ers_year_2 >> task_gen_summary_24mths

    task_gen_summary_24mths >> task_gen_summary_12mths >> task_gen_summary_ytd >> task_load_net_revenue

    AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{DATAMART_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

    AllTaskSuccess.set_upstream([task_load_members_inforce, task_load_net_revenue])
