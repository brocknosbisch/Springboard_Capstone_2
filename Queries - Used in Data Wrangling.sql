
--20190925 bn - Updated logic to only get aggregated data from 3/1/2019 and before.
-- Final SQL will need to use a date sent from Python code so the code will be dynamic.  
-- I am only using 3/1/2019 for the Capstone.

drop table wc_bn_sb_proj_tmp1;

create table wc_bn_sb_proj_tmp1 nologging 
as 
--Customer Information
select
    a.cis_division,
    a.account_id,
    a.customer_class_code,
    a.customer_class_description,
    a.person_id,
    b.sa_id,
    b.start_date sa_start_date,
	to_char(b.start_date,'YYYY') sa_start_year, --20190827 bn
	to_char(b.start_date,'YYYYMM') sa_start_year_month, --20190827 bn
    nvl(b.end_date, to_date('12/31/2099','mm/dd/yyyy')) sa_end_date, --20190827 bn 
	to_char(nvl(b.end_date, to_date('12/31/2099','mm/dd/yyyy')),'YYYY') sa_end_year, --20190827 bn
	to_char(nvl(b.end_date, to_date('12/31/2099','mm/dd/yyyy')),'YYYYMM') sa_end_year_month, --20190827 bn
    b.sa_status_flag,
    
    b.sa_type_code,
    b.rate_class_code,
    b.rate_class_description,
    c.premise_id,
    upper(c.city) city,
    c.state,
    substr(c.postal,0,5) postal,
    a.bill_cycle_code,
    a.bill_cycle_description,
    b.service_type_code,
    c.premise_type_code,
    c.trend_area_code, --to be used for Weather.
    c.office_location,
    c.office_location_description
    --20190919 bn - d."Lat", -- remove for PII
    --20190919 bn - d."Lon"  -- remove for PII
from
    mdw.wc_ccb_account_d a,
    mdw.wc_ccb_sa_d b,
    mdw.wc_ccb_premise_d c,
    mdw_tabl.WC_TABL_CCB_PREM_GEO_S d
where 1=1
    and a.integration_id = b.account_id
    and b.characteristic_premise_id = c.integration_id
    and c.integration_id = d.premise_id (+)
	and b.service_type_code in ('G') --20190826 bn - Gas Only
;


drop table wc_bn_sb_proj_weath_tmp2;

create table wc_bn_sb_proj_weath_tmp2 nologging 
as 
--Historical Weather
select
    a.trend_area_code,
	a.degree_day_date,
    a.average_temperature, 
    a.degree_day,
    a.minimum_temperature,
    a.maximum_temperature
from
    mdw.wc_ccb_deg_day_s a
;


drop table wc_bn_sb_proj_TONN_tmp3;

create table wc_bn_sb_proj_TONN_tmp3 nologging 
as 
--Turn Ons
select 
    a.month_start_date,
    a.sa_start_date,
    a.sa_end_date,
    b.account_id,
    a.sa_id,
    a.premise_id,
    nvl(a.dnp_started_flag, 'N') dnp_started_flag --20190925 bn - added nvl --Did the customer previously DNP?
from
    mdw.wc_tabl_cust_count_recdet_sf a,
    mdw.wc_ccb_sa_s b
where 1=1
    and a.started_flag = 'Y'
    and a.sa_id = b.integration_id
;


drop table wc_bn_sb_proj_TOFF_tmp4;

create table wc_bn_sb_proj_TOFF_tmp4 nologging 
as 
--Turn Off
select 
    a.month_start_date,
    a.sa_start_date,
    a.sa_end_date,
    b.account_id,
    a.sa_id,
    a.premise_id,
    a.dnp_stopped_flag --Did the customer previously DNP?
    
from
    mdw.wc_tabl_cust_count_recdet_sf a,
    mdw.wc_ccb_sa_s b
where 1=1
    and a.stopped_flag = 'Y'
    and a.sa_id = b.integration_id
;


drop table wc_bn_sb_proj_CRED_tmp5;

create table wc_bn_sb_proj_CRED_tmp5 nologging 
as 
--Current Credit Rating 
select 
    account_id,
    internal_credit_rating
from 
	mdw.WC_TABL_CURRENT_CREDIT_RATE_F
;
	
	
drop table wc_bn_sb_proj_PREM_DNP_tmp6;

create table wc_bn_sb_proj_PREM_DNP_tmp6 nologging 
as 
--DNP in last 12 months (PREMISE)
select 
    distinct a.premise_id
from 
    mdw.wc_tabl_cust_count_recdet_sf a
where 1=1
    and a.dnp_stopped_flag = 'Y'
    and month_start_date between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-13) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 12 months --20190925 bn - >= add_months(last_day(trunc(sysdate)),-13) + 1 --Since 1st of same month, last year.
;


drop table wc_bn_sb_proj_ACCT_DNP_tmp7;

create table wc_bn_sb_proj_ACCT_DNP_tmp7 nologging 
as 
--DNP in last 12 months (ACCOUNT)
select 
    distinct b.account_id
from 
    mdw.wc_tabl_cust_count_recdet_sf a,
    mdw.wc_ccb_sa_s b
where 1=1
    and a.dnp_stopped_flag = 'Y'
    and month_start_date between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-13) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 12 months --20190925 bn - >= add_months(last_day(trunc(sysdate)),-13) + 1 --Since 1st of same month, last year.
    and a.sa_id = b.integration_id
;


drop table wc_bn_sb_proj_PLEDGE_tmp8;

create table wc_bn_sb_proj_PLEDGE_tmp8 nologging 
as 
--Pledge in last 18 months (PERSON)
select 
    distinct person_id
from mdw.wc_tabl_pledges_sf
where 1=1
    and pledge_dt between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-19) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 18 months --20190925 bn - >= add_months(last_day(trunc(sysdate)),-19) + 1 --Since 1st of month, 18 months ago.
;


drop table wc_bn_sb_proj_BILLPAY_tmp9;

create table wc_bn_sb_proj_BILLPAY_tmp9 nologging 
as 
-- Number of Payments/Bills in last 18 months w/ current owed.
select /*+ parallel(8) */  
    --wc_ccb_ft_s.account_id,
    --wc_ccb_sa_s.account_id,
    wc_ccb_ft_s.sa_id, 
    nvl(count(case when wc_ccb_ft_s.ft_type_flag = 'PS' and post_to_gl_on_or_after_date between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-19) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 18 months --20190925 bn - >= add_months(trunc(sysdate),-18) 
				   then parent_id end),0) payments_in_last_18_months,  --Payments, not Pay Segs
    nvl(count(case when wc_ccb_ft_s.ft_type_flag = 'BS' and post_to_gl_on_or_after_date between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-19) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 18 months --20190925 bn - >= add_months(trunc(sysdate),-18) 
				   then parent_id end),0) bills_in_last_18_months, --Bills, not BSEGs
    nvl(count(case when wc_ccb_ft_s.ft_type_flag = 'PS' and post_to_gl_on_or_after_date between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-19) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 18 months --20190925 bn - >= add_months(trunc(sysdate),-18) 
				   then sibling_id end),0) pay_segs_in_last_18_months,  --Pay Segs
    nvl(count(case when wc_ccb_ft_s.ft_type_flag = 'BS' and post_to_gl_on_or_after_date between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-19) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 18 months --20190925 bn - >= add_months(trunc(sysdate),-18) 
				   then sibling_id end),0) bill_segs_in_last_18_months, --BSEGs
    sum(case when to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - trunc(sysdate) 
						> trunc(arrears_date) then nvl(wc_ccb_ft_s.current_amount, 0) else 0 end) arrears_current_amount,
    sum(case when to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - trunc(sysdate) 
						> trunc(arrears_date) then nvl(wc_ccb_ft_s.payoff_amount, 0) else 0 end) arrears_payoff_amount,
    sum(nvl(wc_ccb_ft_s.current_amount,0)) total_current_amount,
    sum(nvl(wc_ccb_ft_s.payoff_amount,0)) total_payoff_amount
from 
    mdw.wc_ccb_ft_s--,
    --mdw.wc_ccb_sa_s
where 1=1
    --and wc_ccb_sa_s.integration_id = wc_ccb_ft_s.sa_id
    and wc_ccb_ft_s.frozen_posted_to_account_sw = 'Y'
	and nvl(post_to_gl_on_or_after_date, to_date('1/1/1950','mm/dd/yyyy') ) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - aggregate up to 3/1/2019
group by
    --wc_ccb_sa_s.account_id,
    wc_ccb_ft_s.sa_id
;


drop table wc_bn_sb_proj_PAYDATE_tmp10;

create table wc_bn_sb_proj_PAYDATE_tmp10 nologging 
as 
--Get most recent Payment Date (not cancelled) for each SA.
select /*+ parallel(8) */  
    wc_ccb_ft_s.sa_id, 
    max(wc_ccb_pay_event_s.payment_date) payment_date

from 
	mdw.wc_ccb_ft_s, mdw.wc_ccb_ft_s cancelled_pay,
    mdw.wc_ccb_pay_seg_s,
    mdw.wc_ccb_pay_s,
    mdw.wc_ccb_pay_event_s
where 1=1
    and wc_ccb_ft_s.frozen_posted_to_account_sw = 'Y'
    and wc_ccb_ft_s.ft_type_flag = 'PS'
    and wc_ccb_ft_s.sibling_id = cancelled_pay.sibling_id (+)
    and cancelled_pay.ft_type_flag (+) = 'PX'
    and cancelled_pay.integration_id is null 
    
    and wc_ccb_ft_s.sibling_id = wc_ccb_pay_seg_s.integration_id 
    and wc_ccb_pay_seg_s.payment_id = wc_ccb_pay_s.integration_id
    and wc_ccb_pay_s.pay_event_id = wc_ccb_pay_event_s.integration_Id
	
	and wc_ccb_pay_event_s.payment_date < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - only get max up to 3/1/2019
	
group by wc_ccb_ft_s.sa_id
;


drop table wc_bn_sb_proj_LPC_tmp11;

create table wc_bn_sb_proj_LPC_tmp11 nologging 
as 
--Late Payments in last 18 months (SA)
select 
	sa_id, 
	count(1) lpc_count 
From mdw.wc_ccb_adj_s 
where 1=1
    and adjustment_type_code = 'LPC' 
    and adjustment_status_flag = '50' 
    and creation_date between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-19) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 18 months --20190925 bn - >= add_months(trunc(sysdate), -18)
group by sa_id
;


drop table wc_bn_sb_proj_SEAS_1_tmp12;

create table wc_bn_sb_proj_SEAS_1_tmp12 nologging 
as 
--Seasonal Premise - Prior Year
select 
    distinct turnon.premise_id
from 
    mdw.wc_tabl_cust_count_recdet_sf turnon, 
    mdw.wc_ccb_sa_s turnon_sa,
    mdw.wc_tabl_cust_count_recdet_sf turnoff,
    mdw.wc_ccb_sa_s turnoff_sa
    
where 1=1
    and turnon.started_flag = 'Y'
    and turnoff.stopped_flag = 'Y'
    and turnon.premise_id = turnoff.premise_id
    and turnon.sa_id = turnon_sa.integration_id
    and turnoff.sa_id = turnoff_sa.integration_id
    and turnoff.month_start_date between to_date('3/1/' || to_char(add_months(sysdate,-12), 'YYYY'), 'MM/DD/YYYY')
                                        and to_date('6/30/' || to_char(add_months(sysdate,-12), 'YYYY'), 'MM/DD/YYYY')
    and turnon.month_start_date between to_date('8/1/' || to_char(add_months(sysdate,-12), 'YYYY'), 'MM/DD/YYYY')
                                        and to_date('12/31/' || to_char(add_months(sysdate,-12), 'YYYY'), 'MM/DD/YYYY')
group by turnon.premise_id
;


drop table wc_bn_sb_proj_SEAS_2_tmp13;

create table wc_bn_sb_proj_SEAS_2_tmp13 nologging 
as 
--Seasonal Premise - 2 Years Ago
select 
    distinct turnon.premise_id
from 
    mdw.wc_tabl_cust_count_recdet_sf turnon, 
    mdw.wc_ccb_sa_s turnon_sa,
    mdw.wc_tabl_cust_count_recdet_sf turnoff,
    mdw.wc_ccb_sa_s turnoff_sa
    
where 1=1
    and turnon.started_flag = 'Y'
    and turnoff.stopped_flag = 'Y'
    and turnon.premise_id = turnoff.premise_id
    and turnon.sa_id = turnon_sa.integration_id
    and turnoff.sa_id = turnoff_sa.integration_id
    and turnoff.month_start_date between to_date('3/1/' || to_char(add_months(sysdate,-24), 'YYYY'), 'MM/DD/YYYY')
                                        and to_date('6/30/' || to_char(add_months(sysdate,-24), 'YYYY'), 'MM/DD/YYYY')
    and turnon.month_start_date between to_date('8/1/' || to_char(add_months(sysdate,-24), 'YYYY'), 'MM/DD/YYYY')
                                        and to_date('12/31/' || to_char(add_months(sysdate,-24), 'YYYY'), 'MM/DD/YYYY')
group by turnon.premise_id
;


drop table wc_bn_sb_proj_SEAS_3_tmp14;

create table wc_bn_sb_proj_SEAS_3_tmp14 nologging 
as 
--Seasonal Premise - 3 Years Ago
select 
    distinct turnon.premise_id
from 
    mdw.wc_tabl_cust_count_recdet_sf turnon, 
    mdw.wc_ccb_sa_s turnon_sa,
    mdw.wc_tabl_cust_count_recdet_sf turnoff,
    mdw.wc_ccb_sa_s turnoff_sa
    
where 1=1
    and turnon.started_flag = 'Y'
    and turnoff.stopped_flag = 'Y'
    and turnon.premise_id = turnoff.premise_id
    and turnon.sa_id = turnon_sa.integration_id
    and turnoff.sa_id = turnoff_sa.integration_id
    and turnoff.month_start_date between to_date('3/1/' || to_char(add_months(sysdate,-36), 'YYYY'), 'MM/DD/YYYY')
                                        and to_date('6/30/' || to_char(add_months(sysdate,-36), 'YYYY'), 'MM/DD/YYYY')
    and turnon.month_start_date between to_date('8/1/' || to_char(add_months(sysdate,-36), 'YYYY'), 'MM/DD/YYYY')
                                        and to_date('12/31/' || to_char(add_months(sysdate,-36), 'YYYY'), 'MM/DD/YYYY')
group by turnon.premise_id
;


drop table wc_bn_sb_proj_tmp15;

create table wc_bn_sb_proj_tmp15 nologging
as --compbining temp tables
select distinct --20190826 bn - Need to put distinct because some SAs in wc_tabl_cust_count_recdet_sf have multiple premises. 
    a.*,
    nvl(b.dnp_started_flag, 'N') dnp_started_flag, --20190827 bn 
    nvl(c.dnp_stopped_flag, 'N') dnp_stopped_flag, --20190827 bn 
    nvl(d.internal_credit_rating, -1) internal_credit_rating, --20190827 bn 
    (case when e.premise_id is not null then 'Y' else 'N' end) premise_level_12_mth_dnp_flag,
    (case when f.account_id is not null then 'Y' else 'N' end) account_level_12_mth_dnp_flag
from
    wc_bn_sb_proj_tmp1 a,
    wc_bn_sb_proj_TONN_tmp3 b,
    wc_bn_sb_proj_TOFF_tmp4 c,
    wc_bn_sb_proj_CRED_tmp5 d,
    wc_bn_sb_proj_PREM_DNP_tmp6 e,
    wc_bn_sb_proj_ACCT_DNP_tmp7 f
where 1=1
    and a.sa_id = b.sa_id (+)
    and a.sa_id = c.sa_id (+)
    and a.account_id = d.account_id (+)
    and a.premise_id = e.premise_id (+)
    and a.account_id = f.account_id (+)
;


drop table wc_bn_sb_proj_tmp16;

create table wc_bn_sb_proj_tmp16 nologging
as --compbining the rest of the temp tables
select
    a.*,
    (case when b.person_id is not null then 'Y' else 'N' end) person_rcvd_18_mths_pledge,
    nvl(c.payments_in_last_18_months,0) payments_in_last_18_months, --20190827 bn 
    nvl(c.bills_in_last_18_months,0) bills_in_last_18_months, --20190827 bn 
    nvl(c.pay_segs_in_last_18_months,0) pay_segs_in_last_18_months, --20190827 bn 
    nvl(c.bill_segs_in_last_18_months,0) bill_segs_in_last_18_months, --20190827 bn 
    nvl(c.arrears_current_amount,0) arrears_current_amount, --20190827 bn 
    nvl(c.arrears_payoff_amount,0) arrears_payoff_amount, --20190827 bn 
    nvl(c.total_current_amount,0) total_current_amount, --20190827 bn 
    nvl(c.total_payoff_amount,0) total_payoff_amount, --20190827 bn 
    d.payment_date most_recent_payment_date,
    nvl(e.lpc_count,0) late_payment_count, --20190827 bn 
    (case when f.premise_id is not null then 'Y' else 'N' end) seasonal_prior_1_yr_flag,
    (case when g.premise_id is not null then 'Y' else 'N' end) seasonal_prior_2_yr_flag,
    (case when h.premise_id is not null then 'Y' else 'N' end) seasonal_prior_3_yr_flag
    
from
    wc_bn_sb_proj_tmp15 a,
    wc_bn_sb_proj_PLEDGE_tmp8 b,
    wc_bn_sb_proj_BILLPAY_tmp9 c,
    wc_bn_sb_proj_PAYDATE_tmp10 d,
    wc_bn_sb_proj_LPC_tmp11 e,
    wc_bn_sb_proj_SEAS_1_tmp12 f,
    wc_bn_sb_proj_SEAS_2_tmp13 g,
    wc_bn_sb_proj_SEAS_3_tmp14 h
where 1=1
    and a.person_id = b.person_id (+)
    and a.sa_id = c.sa_id (+)
    and a.sa_id = d.sa_id (+)
    and a.sa_id = e.sa_id (+)
    and a.premise_id = f.premise_id (+)
    and a.premise_id = g.premise_id (+)
    and a.premise_id = h.premise_id (+)
;	
	
	
drop table wc_bn_sb_proj_tmp17;

create table wc_bn_sb_proj_tmp17 nologging
as --combining data with degree days to get the degree days at start/end sa dates.
select 
    a.*,
    nvl(b.degree_day,-1) sa_start_degree_day, --20190827 bn 
    nvl(b.average_temperature, -1) sa_start_avg_temp, --20190827 bn 
    nvl(c.degree_day, -1) sa_end_degree_day, --20190827 bn 
    nvl(c.average_temperature, -1) sa_end_avg_temp, --20190827 bn 
	(case when sa_end_year = '2013' then 1 else 0 end) STOP_2013, --20190827 bn 
	(case when sa_end_year = '2014' then 1 else 0 end) STOP_2014, --20190827 bn 
	(case when sa_end_year = '2015' then 1 else 0 end) STOP_2015, --20190827 bn 
	(case when sa_end_year = '2016' then 1 else 0 end) STOP_2016, --20190827 bn 
	(case when sa_end_year = '2017' then 1 else 0 end) STOP_2017, --20190827 bn 
	(case when sa_end_year = '2018' then 1 else 0 end) STOP_2018, --20190827 bn 
	(case when sa_end_year = '2019' then 1 else 0 end) STOP_2019,  --20190827 bn 
	(case when sa_start_year = '2013' then 1 else 0 end) START_2013, --20190911 bn 
	(case when sa_start_year = '2014' then 1 else 0 end) START_2014, --20190911 bn 
	(case when sa_start_year = '2015' then 1 else 0 end) START_2015, --20190911 bn 
	(case when sa_start_year = '2016' then 1 else 0 end) START_2016, --20190911 bn 
	(case when sa_start_year = '2017' then 1 else 0 end) START_2017, --20190911 bn 
	(case when sa_start_year = '2018' then 1 else 0 end) START_2018, --20190911 bn 
	(case when sa_start_year = '2019' then 1 else 0 end) START_2019  --20190911 bn 
from
    wc_bn_sb_proj_tmp16 a,
    wc_bn_sb_proj_weath_tmp2 b,
    wc_bn_sb_proj_weath_tmp2 c
where 1=1
    and b.trend_area_code (+) = 'LGC'
    and c.trend_area_code (+) = 'LGC'
    and a.sa_start_date = b.degree_day_date (+)
    and a.sa_end_date = c.degree_day_date (+)
;


drop table wc_bn_sb_proj_tmp18; 

--20190911 bn 
create table wc_bn_sb_proj_tmp18 nologging as
select 
    c1sa sa_id,
    max(case when to_char(pledge_dt, 'YYYY') = '2013' then trunc(pledge_dt) else null end) pledge_date_2013,
    max(case when to_char(pledge_dt, 'YYYY') = '2013' then 1 else 0 end) pledge_flag_2013,
    max(case when to_char(pledge_dt, 'YYYY') = '2014' then trunc(pledge_dt) else null end) pledge_date_2014,
    max(case when to_char(pledge_dt, 'YYYY') = '2014' then 1 else 0 end) pledge_flag_2014,
    max(case when to_char(pledge_dt, 'YYYY') = '2015' then trunc(pledge_dt) else null end) pledge_date_2015,
    max(case when to_char(pledge_dt, 'YYYY') = '2015' then 1 else 0 end) pledge_flag_2015,
    max(case when to_char(pledge_dt, 'YYYY') = '2016' then trunc(pledge_dt) else null end) pledge_date_2016,
    max(case when to_char(pledge_dt, 'YYYY') = '2016' then 1 else 0 end) pledge_flag_2016,
    max(case when to_char(pledge_dt, 'YYYY') = '2017' then trunc(pledge_dt) else null end) pledge_date_2017,
    max(case when to_char(pledge_dt, 'YYYY') = '2017' then 1 else 0 end) pledge_flag_2017,
    max(case when to_char(pledge_dt, 'YYYY') = '2018' then trunc(pledge_dt) else null end) pledge_date_2018,
    max(case when to_char(pledge_dt, 'YYYY') = '2018' then 1 else 0 end) pledge_flag_2018,
    max(case when to_char(pledge_dt, 'YYYY') = '2019' then trunc(pledge_dt) else null end) pledge_date_2019,
    max(case when to_char(pledge_dt, 'YYYY') = '2019' then 1 else 0 end) pledge_flag_2019
From WC_TABL_PLEDGES_SF
group by c1sa;



drop table wc_bn_sb_proj_tmp19; 

--20190911 bn 
create table wc_bn_sb_proj_tmp19 nologging as
select
    sa_id,
    sum(billed_usage) usage
from mdw.wc_tabl_usage_revenue_sf_v
where last_day_of_period between add_months(last_day(to_date('3/1/2019','mm/dd/yyyy')),-19) + 1 and to_date('2/28/2019','mm/dd/yyyy') --20190925 bn - 18 months --20190925 bn - >= add_months(trunc(sysdate),-18)
group by sa_id
;


drop table wc_bn_sb_proj_tmp20; 

--20190911 bn 
create table wc_bn_sb_proj_tmp20 nologging as
select 
	a.*,
	b.pledge_date_2013,
	nvl(b.pledge_flag_2013, 0) pledge_flag_2013,
	b.pledge_date_2014,
	nvl(b.pledge_flag_2014, 0) pledge_flag_2014,
	b.pledge_date_2015,
	nvl(b.pledge_flag_2015, 0) pledge_flag_2015,
	b.pledge_date_2016,
	nvl(b.pledge_flag_2016, 0) pledge_flag_2016,
	b.pledge_date_2017,
	nvl(b.pledge_flag_2017, 0) pledge_flag_2017,
	b.pledge_date_2018,
	nvl(b.pledge_flag_2018, 0) pledge_flag_2018,
	b.pledge_date_2019,
	nvl(b.pledge_flag_2019, 0) pledge_flag_2019,
	c.usage usage_in_last_18_months 
from	
	wc_bn_sb_proj_tmp17 a,
	wc_bn_sb_proj_tmp18 b,
	wc_bn_sb_proj_tmp19 c
where 1=1
	and a.sa_id = b.sa_id (+)
	and a.sa_id = c.sa_id (+)
;


drop table wc_bn_sb_proj_tmp21; 

--20190912 bn - Get Min/Max SA Start/End Dates by Person ID
create table wc_bn_sb_proj_tmp21 nologging as
select 
    person_id, 
    max(case when trunc(a.sa_start_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_start_date else null end) max_sa_start_date, 
    min(case when trunc(a.sa_start_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_start_date else null end) min_sa_start_date,
    max(case when trunc(sa_end_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_end_date else null end) max_sa_end_date,
    min(case when trunc(sa_end_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_end_date else null end) min_sa_end_date
from
    wc_bn_sb_proj_tmp1 a
where 1=1
    and sa_status_flag <> '70' --20190919 bn 
group by 
    person_id
;


drop table wc_bn_sb_proj_tmp22; 

--20190912 bn - Get Min/Max SA Start/End Dates by Premise ID
create table wc_bn_sb_proj_tmp22 nologging as
select 
    premise_id, 
    max(case when trunc(a.sa_start_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_start_date else null end) max_sa_start_date, 
    min(case when trunc(a.sa_start_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_start_date else null end) min_sa_start_date,
    max(case when trunc(sa_end_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_end_date else null end) max_sa_end_date,
    min(case when trunc(sa_end_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 
		then sa_end_date else null end) min_sa_end_date
from
    wc_bn_sb_proj_tmp1 a
where 1=1
    and sa_status_flag <> '70' --20190919 bn 
group by 
    premise_id
;


drop table wc_bn_sb_proj_tmp23; 

--20190912 bn - Get Min/Max SA Start/End Dates by Premise ID
create table wc_bn_sb_proj_tmp23 nologging as
select
    a.*,
    trunc(per.max_sa_start_date) person_max_sa_start_date, 
    trunc(per.min_sa_start_date) person_min_sa_start_date,
    trunc(per.max_sa_end_date) person_max_sa_end_date,
    trunc(per.min_sa_end_date) person_min_sa_end_date,
    trunc(prem.max_sa_start_date) premise_max_sa_start_date, 
    trunc(prem.min_sa_start_date) premise_min_sa_start_date,
    trunc(prem.max_sa_end_date) premise_max_sa_end_date,
    trunc(prem.min_sa_end_date) premise_min_sa_end_date
from
    wc_bn_sb_proj_tmp20 a,
    wc_bn_sb_proj_tmp21 per,
    wc_bn_sb_proj_tmp22 prem
where 1=1
    and a.person_id = per.person_id (+)
    and a.premise_id = prem.premise_id (+) 
;


drop table wc_bn_sb_proj_tmp24; 

--20190912 bn - Add Prior Stop Date and Number of Days Inactive
create table wc_bn_sb_proj_tmp24 nologging as
select /*+ parallel(4) */ 
    a.*,
    nvl(sa_start_date - premise_prior_stop_date,0) premise_days_inactive_before,
    nvl(sa_start_date - person_prior_stop_date,0) person_days_inactive_before,
    nvl(premise_prior_stop_date - premise_prior_start_date,0) premise_days_active_before, --20190925 bn 
    nvl(person_prior_stop_date - person_prior_start_date,0) person_days_active_before, --20190925 bn 
	(case when sa_start_date < to_date('3/1/2019','mm/dd/yyyy') and sa_end_date >= to_date('3/1/2019','mm/dd/yyyy')
		  then to_date('3/1/2019','mm/dd/yyyy') - sa_start_date 
		  else null
	 end) ACTIVE_DIFF_FROM_20190301 --20190925 bn - Change this dynamically from python later.
from
    (
        select  /*+ parallel(4) */ 
            a.*,
            LAG (trunc(a.sa_end_date), 1)
            OVER (PARTITION BY a.premise_id
                  ORDER BY a.sa_start_date) premise_prior_stop_date,
            LAG (trunc(a.sa_end_date), 1)
            OVER (PARTITION BY a.person_id
                  ORDER BY a.sa_start_date) person_prior_stop_date,
            LAG (trunc(a.sa_start_date), 1)
            OVER (PARTITION BY a.premise_id
                  ORDER BY a.sa_start_date) premise_prior_start_date, --20190925 bn
            LAG (trunc(a.sa_start_date), 1)
            OVER (PARTITION BY a.person_id
                  ORDER BY a.sa_start_date) person_prior_start_date --20190925 bn
        from
            wc_bn_sb_proj_tmp23 a
        where trunc(a.sa_start_date) < to_date('3/1/2019','mm/dd/yyyy') --20190925 bn - sysdate+180 --20190919 bn - There were bad dates so need to add filter to only get 180 days.
    ) a
;


--20190919 bn - masking
CREATE OR REPLACE FORCE VIEW MDW_BI_READ.WC_BN_SB_PROJ_TMP26_V
(
    CIS_DIVISION,
    ACCOUNT_ID,
    CUSTOMER_CLASS_CODE,
    CUSTOMER_CLASS_DESCRIPTION,
    PERSON_ID,
    SA_ID,
    SA_START_DATE,
    SA_START_YEAR,
    SA_START_YEAR_MONTH,
    SA_END_DATE,
    SA_END_YEAR,
    SA_END_YEAR_MONTH,
    SA_STATUS_FLAG,
    SA_TYPE_CODE,
    RATE_CLASS_CODE,
    RATE_CLASS_DESCRIPTION,
    PREMISE_ID,
    CITY,
    STATE,
    POSTAL,
    BILL_CYCLE_CODE,
    BILL_CYCLE_DESCRIPTION,
    SERVICE_TYPE_CODE,
    PREMISE_TYPE_CODE,
    TREND_AREA_CODE,
    OFFICE_LOCATION,
    OFFICE_LOCATION_DESCRIPTION,
    DNP_STARTED_FLAG,
    DNP_STOPPED_FLAG,
    INTERNAL_CREDIT_RATING,
    PREMISE_LEVEL_12_MTH_DNP_FLAG,
    ACCOUNT_LEVEL_12_MTH_DNP_FLAG,
    PERSON_RCVD_18_MTHS_PLEDGE,
    PAYMENTS_IN_LAST_18_MONTHS,
    BILLS_IN_LAST_18_MONTHS,
    PAY_SEGS_IN_LAST_18_MONTHS,
    BILL_SEGS_IN_LAST_18_MONTHS,
    ARREARS_CURRENT_AMOUNT,
    ARREARS_PAYOFF_AMOUNT,
    TOTAL_CURRENT_AMOUNT,
    TOTAL_PAYOFF_AMOUNT,
    MOST_RECENT_PAYMENT_DATE,
    LATE_PAYMENT_COUNT,
    SEASONAL_PRIOR_1_YR_FLAG,
    SEASONAL_PRIOR_2_YR_FLAG,
    SEASONAL_PRIOR_3_YR_FLAG,
    SA_START_DEGREE_DAY,
    SA_START_AVG_TEMP,
    SA_END_DEGREE_DAY,
    SA_END_AVG_TEMP,
    STOP_2013,
    STOP_2014,
    STOP_2015,
    STOP_2016,
    STOP_2017,
    STOP_2018,
    STOP_2019,
    START_2013,
    START_2014,
    START_2015,
    START_2016,
    START_2017,
    START_2018,
    START_2019,
    PLEDGE_DATE_2013,
    PLEDGE_FLAG_2013,
    PLEDGE_DATE_2014,
    PLEDGE_FLAG_2014,
    PLEDGE_DATE_2015,
    PLEDGE_FLAG_2015,
    PLEDGE_DATE_2016,
    PLEDGE_FLAG_2016,
    PLEDGE_DATE_2017,
    PLEDGE_FLAG_2017,
    PLEDGE_DATE_2018,
    PLEDGE_FLAG_2018,
    PLEDGE_DATE_2019,
    PLEDGE_FLAG_2019,
    USAGE_IN_LAST_18_MONTHS,
    PERSON_MAX_SA_START_DATE,
    PERSON_MIN_SA_START_DATE,
    PERSON_MAX_SA_END_DATE,
    PERSON_MIN_SA_END_DATE,
    PREMISE_MAX_SA_START_DATE,
    PREMISE_MIN_SA_START_DATE,
    PREMISE_MAX_SA_END_DATE,
    PREMISE_MIN_SA_END_DATE,
    PREMISE_PRIOR_STOP_DATE,
    PERSON_PRIOR_STOP_DATE,
    PREMISE_DAYS_INACTIVE_BEFORE,
    PERSON_DAYS_INACTIVE_BEFORE,
    --20190925 bn - duplicate column - USAGE_PAST_18_MONTHS,
	premise_days_active_before, --20190925 bn 
	person_days_active_before, --20190925 bn 
	ACTIVE_DIFF_FROM_20190301 --20190925 bn 
)
BEQUEATH DEFINER
AS
    SELECT CIS_DIVISION,
           ############## ACCOUNT_ID, --20190926 bn - removed masking logic for publishing SQL
           CUSTOMER_CLASS_CODE,
           CUSTOMER_CLASS_DESCRIPTION,
           ############## PERSON_ID, --20190926 bn - removed masking logic for publishing SQL
           ############## SA_ID, --20190926 bn - removed masking logic for publishing SQL
           SA_START_DATE,
           SA_START_YEAR,
           SA_START_YEAR_MONTH,
           SA_END_DATE,
           SA_END_YEAR,
           SA_END_YEAR_MONTH,
           SA_STATUS_FLAG,
           SA_TYPE_CODE,
           RATE_CLASS_CODE,
           RATE_CLASS_DESCRIPTION,
           ############## PREMISE_ID, --20190926 bn - removed masking logic for publishing SQL
           CITY,
           STATE,
           POSTAL,
           BILL_CYCLE_CODE,
           BILL_CYCLE_DESCRIPTION,
           SERVICE_TYPE_CODE,
           PREMISE_TYPE_CODE,
           TREND_AREA_CODE,
           OFFICE_LOCATION,
           OFFICE_LOCATION_DESCRIPTION,
           DNP_STARTED_FLAG,
           DNP_STOPPED_FLAG,
           INTERNAL_CREDIT_RATING,
           PREMISE_LEVEL_12_MTH_DNP_FLAG,
           ACCOUNT_LEVEL_12_MTH_DNP_FLAG,
           PERSON_RCVD_18_MTHS_PLEDGE,
           PAYMENTS_IN_LAST_18_MONTHS,
           BILLS_IN_LAST_18_MONTHS,
           PAY_SEGS_IN_LAST_18_MONTHS,
           BILL_SEGS_IN_LAST_18_MONTHS,
           ARREARS_CURRENT_AMOUNT,
           ARREARS_PAYOFF_AMOUNT,
           TOTAL_CURRENT_AMOUNT,
           TOTAL_PAYOFF_AMOUNT,
           MOST_RECENT_PAYMENT_DATE,
           LATE_PAYMENT_COUNT,
           SEASONAL_PRIOR_1_YR_FLAG,
           SEASONAL_PRIOR_2_YR_FLAG,
           SEASONAL_PRIOR_3_YR_FLAG,
           SA_START_DEGREE_DAY,
           SA_START_AVG_TEMP,
           SA_END_DEGREE_DAY,
           SA_END_AVG_TEMP,
           STOP_2013,
           STOP_2014,
           STOP_2015,
           STOP_2016,
           STOP_2017,
           STOP_2018,
           STOP_2019,
           START_2013,
           START_2014,
           START_2015,
           START_2016,
           START_2017,
           START_2018,
           START_2019,
           PLEDGE_DATE_2013,
           PLEDGE_FLAG_2013,
           PLEDGE_DATE_2014,
           PLEDGE_FLAG_2014,
           PLEDGE_DATE_2015,
           PLEDGE_FLAG_2015,
           PLEDGE_DATE_2016,
           PLEDGE_FLAG_2016,
           PLEDGE_DATE_2017,
           PLEDGE_FLAG_2017,
           PLEDGE_DATE_2018,
           PLEDGE_FLAG_2018,
           PLEDGE_DATE_2019,
           PLEDGE_FLAG_2019,
           USAGE_IN_LAST_18_MONTHS,
           PERSON_MAX_SA_START_DATE,
           PERSON_MIN_SA_START_DATE,
           PERSON_MAX_SA_END_DATE,
           PERSON_MIN_SA_END_DATE,
           PREMISE_MAX_SA_START_DATE,
           PREMISE_MIN_SA_START_DATE,
           PREMISE_MAX_SA_END_DATE,
           PREMISE_MIN_SA_END_DATE,
           case when PREMISE_PRIOR_STOP_DATE > sysdate then null 
				else PREMISE_PRIOR_STOP_DATE
		   end PREMISE_PRIOR_STOP_DATE, --20190925 bn 
           case when PERSON_PRIOR_STOP_DATE > sysdate then null 
				else PERSON_PRIOR_STOP_DATE
		   end PERSON_PRIOR_STOP_DATE, --20190925 bn 
           case when PREMISE_PRIOR_STOP_DATE > sysdate then 0
				else PREMISE_DAYS_INACTIVE_BEFORE
		   end PREMISE_DAYS_INACTIVE_BEFORE, --20190925 bn 
           case when PERSON_PRIOR_STOP_DATE > sysdate then 0 
				else PERSON_DAYS_INACTIVE_BEFORE
		   end PERSON_DAYS_INACTIVE_BEFORE, --20190925 bn 
           --20190925 bn - duplicate column - USAGE_PAST_18_MONTHS,
			case when PREMISE_PRIOR_STOP_DATE > sysdate then 0
				else premise_days_active_before
		    end premise_days_active_before, --20190925 bn 
			case when PERSON_PRIOR_STOP_DATE > sysdate then 0 
				else person_days_active_before
		    end person_days_active_before, --20190925 bn 
			ACTIVE_DIFF_FROM_20190301 --20190925 bn 
      FROM wc_bn_sb_proj_tmp24; --20190925 bn - changed to tmp24.
	  

--20190826 bn - EXPORTING this data to CSV so that I can bring load into my Jupyter notebook.
select * from wc_bn_sb_proj_tmp26_v
where city in 
	(	'INDEPENDENCE',
		'SAINT CHARLES',
		'LEES SUMMIT',
		'BALLWIN',
		'SAINT JOSEPH',
		'O FALLON',
		'SAINT PETERS',
		'JOPLIN',
		'BLUE SPRINGS',
		'CHESTERFIELD',
		'RAYTOWN',
		'WEBB CITY',
		'IMPERIAL',
		'PARKVILLE',
		'EUREKA',
		'GROVER',
		'AURORA',
		'VALLEY PARK',
		'MONETT',
		'ELLISVILLE',
		'CAMERON'
	)
;


Export dataset to Delimited Text, Double Quote Strings, Character Comma, Include column headers.
