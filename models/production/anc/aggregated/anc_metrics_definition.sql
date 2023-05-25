{{ config(
  materialized='table',
  indexes=[
      {'columns': ['month_start_date']}
    ],
) }}

-- create monthly calendar
WITH calendar AS (
SELECT d::date as month_start_date,(d + '1 month'::interval - '1 day'::interval )::date AS month_end_date
FROM generate_series('2022-01-01',  now(), '1 month'::interval)d
),

-- determine anc open count for each month
anc_open_count AS (SELECT calendar.month_start_date, count(distinct c.id) AS open_case_count
FROM calendar
LEFT JOIN  
(SELECT id,month_start_date, open_status, program_code,clustername,coid,aww_number 
FROM {{ref('anc_case_open')}}) c 
ON calendar.month_start_date=c.month_start_date
where c.open_status is TRUE
GROUP BY calendar.month_start_date),

-- total unique visits 
visits_counts AS (
SELECT month_start_date, count(distinct caseid) AS total_visits_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC'
GROUP BY month_start_date),

visits_counts_co AS (
SELECT month_start_date, count(distinct caseid) AS co_visits_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC' AND conducted_by='co'
GROUP BY month_start_date),

visits_counts_volunteer AS (
SELECT  month_start_date, count(distinct caseid) AS volunteer_visits_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC' AND conducted_by='volunteer'
GROUP BY month_start_date),

-- volunteer count
-- determine volunteer open count for each month
volunteers_open AS 
(SELECT month_start_date, count(distinct id) AS volunteer_open_count 
FROM {{ref('volunteer_open_case')}}
WHERE sex ='female'
GROUP BY month_start_date),

-- unique volunteers with referrals
volunteers_with_referrals as (
SELECT month_start_date, count(distinct volunteerid) as volunteers_involved_in_anc_referral_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC' AND referral='Yes' AND conducted_by ='volunteer'
-- needs confirmation
--AND (referral_reasons LIKE '%anc_reg%' OR referral_reasons LIKE '%anc_service%' )
GROUP BY month_start_date ),


-- volunteer visits with referrals
volunteer_referrals as (SELECT month_start_date, count(distinct caseid) as volunteer_referral_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC' AND referral='Yes' AND conducted_by ='volunteer' 
-- needs confirmation
--AND (referral_reasons LIKE '%anc_reg%' OR referral_reasons LIKE '%anc_service%' )
GROUP BY month_start_date),


-- co visits with referrals
co_referrals as (SELECT month_start_date, count(distinct caseid) as co_referral_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC' AND referral='Yes' AND conducted_by ='co' 
-- needs confirmation
--AND (referral_reasons LIKE '%anc_reg%' OR referral_reasons LIKE '%anc_service%' )
GROUP BY month_start_date),

-- visits with THR referrals
thr_referrals as (SELECT month_start_date, count(distinct caseid) as thr_referral_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC' AND referral='Yes'  AND (referral_reasons LIKE '%ICDS_THR%' )
GROUP BY month_start_date),

-- determine anc open count who are anemia tested for each month
anc_open_count_anemia_tested AS (SELECT calendar.month_start_date, count(c.id) AS anemia_tested_count
FROM calendar
CROSS JOIN  
(SELECT id, anc_identify_date,anc_close_date,anc_closed,lmpdate,earliest_hb_grade_date FROM {{ref('anc_case')}}) c 
WHERE ((c.anc_identify_date <= calendar.month_end_date) AND
((c.anc_close_date>calendar.month_end_date)OR (c.anc_close_date IS NULL)) AND (c.earliest_hb_grade_date <= calendar.month_end_date))
GROUP BY calendar.month_start_date),

-- determine anc open count distribution of trimester registration
anc_open_count_trimester_anc_registration AS (SELECT calendar.month_start_date, 
sum(case when c.anc_reg_trimester = 'First_trim' then 1 else 0 END) AS early_registration_count,
sum(case when c.anc_reg_trimester = 'First_trim' then 1 else 0 END) AS anc_reg_first_trimester_count,
sum(case when c.anc_reg_trimester = 'Second_trim' then 1 else 0 END) AS anc_reg_second_trimester_count,
sum(case when c.anc_reg_trimester = 'Third_trim' then 1 else 0 END) AS anc_reg_third_trimester_count
FROM calendar
CROSS JOIN  
(SELECT id, anc_identify_date,anc_close_date,anc_closed,lmpdate,anc_reg_trimester FROM {{ref('anc_case')}}) c 
WHERE ((c.anc_identify_date <= calendar.month_end_date) AND
((c.anc_close_date>calendar.month_end_date)OR (c.anc_close_date IS NULL)))
GROUP BY calendar.month_start_date),

-- institutional deliveries
deliveries as (SELECT date_trunc('month', delivery_date ) AS month_start_date, count(*) as delivery_count,
sum(case when delivery_site_type='Institutional' THEN 1 ELSE 0 END) AS institutional_delivery_count
FROM {{ref('anc_outcome')}}
WHERE pregoutcome IN ('livebirth','stillbirth')
GROUP BY date_trunc('month', delivery_date )),

--  visits with THR
thr_visits as (SELECT month_start_date, count(distinct caseid) as thr_count
FROM {{ref('anc_visit')}}
WHERE visitreason='ANC' AND ancthr ='Yes' 
GROUP BY month_start_date)


select 
anc_open_count.month_start_date,
anc_open_count.open_case_count,
visits_counts.total_visits_count, 
-- visits_counts.total_count::float/anc_open_count.open_count as pct_visited, 
visits_counts_co.co_visits_count, 
visits_counts_volunteer.volunteer_visits_count,
volunteers_open.volunteer_open_count,
volunteers_with_referrals.volunteers_involved_in_anc_referral_count,
volunteer_referrals.volunteer_referral_count,
co_referrals.co_referral_count,
anc_open_count_anemia_tested.anemia_tested_count,
anc_open_count_trimester_anc_registration.early_registration_count,
anc_open_count_trimester_anc_registration.anc_reg_first_trimester_count,
anc_open_count_trimester_anc_registration.anc_reg_second_trimester_count,
anc_open_count_trimester_anc_registration.anc_reg_third_trimester_count,
thr_visits.thr_count,
thr_referrals.thr_referral_count,
deliveries.delivery_count,
deliveries.institutional_delivery_count

from anc_open_count
left join visits_counts
on anc_open_count.month_start_date=visits_counts.month_start_date
left join visits_counts_volunteer
on anc_open_count.month_start_date=visits_counts_volunteer.month_start_date
left join visits_counts_co
on anc_open_count.month_start_date=visits_counts_co.month_start_date
left join volunteers_open
on anc_open_count.month_start_date = volunteers_open.month_start_date
left join volunteers_with_referrals
on anc_open_count.month_start_date=volunteers_with_referrals.month_start_date
left join volunteer_referrals
on anc_open_count.month_start_date=volunteer_referrals.month_start_date
left join co_referrals
on anc_open_count.month_start_date=co_referrals.month_start_date
left join anc_open_count_anemia_tested
on anc_open_count.month_start_date=anc_open_count_anemia_tested.month_start_date
left join thr_referrals
on anc_open_count.month_start_date=thr_referrals.month_start_date
left join anc_open_count_trimester_anc_registration
on anc_open_count.month_start_date=anc_open_count_trimester_anc_registration.month_start_date
left join deliveries
on anc_open_count.month_start_date=deliveries.month_start_date
left join thr_visits
on anc_open_count.month_start_date=thr_visits.month_start_date


