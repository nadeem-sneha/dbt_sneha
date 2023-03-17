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
anc_open_count AS (SELECT calendar.month_start_date, count(c.id) AS open_count
FROM calendar
CROSS JOIN  
(SELECT id, anc_identify_date,anc_close_date,anc_closed,lmpdate FROM {{ref('anc_case')}}) c 
WHERE ((c.anc_identify_date <= calendar.month_end_date) AND 
((c.anc_close_date>calendar.month_end_date)OR (c.anc_close_date IS NULL)))

GROUP BY calendar.month_start_date),
-- visit data
visits_casedata as (SELECT date_trunc('month', visitdate ) AS visit_month, caseid, 
case when conducted_by = 'co' then 1 else 0 end AS conducted_by_co,
case when conducted_by = 'volunteer' then 1 else 0 end AS conducted_by_volunteer
FROM {{ref('anc_visit_duplicates_removed')}}
WHERE visitreason='ANC'),

-- visits by COs and CAGs
visits_counts AS (
SELECT calendar.month_start_date, count(distinct caseid) AS total_count, sum(visits_casedata.conducted_by_co) as visits_count_by_co,sum(visits_casedata.conducted_by_volunteer) as visits_count_by_volunteer 
FROM calendar
LEFT JOIN
visits_casedata
ON visits_casedata.visit_month=calendar.month_start_date
GROUP BY calendar.month_start_date),

-- volunteer visits with referrals
volunteer_referrals as (SELECT date_trunc('month', visitdate ) AS month_start_date, count(distinct caseid) as volunteer_referral_count
FROM {{ref('anc_visit_duplicates_removed')}}
WHERE visitreason='ANC' AND referral='Yes' AND conducted_by ='volunteer' AND (referral_reasons LIKE '%anc_reg%' OR referral_reasons LIKE '%anc_service%' )
GROUP BY month_start_date)


select anc_open_count.month_start_date,anc_open_count.open_count,visits_counts.total_count, visits_counts.total_count::float/anc_open_count.open_count as pct_visited, visits_counts.visits_count_by_co, visits_counts.visits_count_by_volunteer, volunteer_referrals.volunteer_referral_count
from anc_open_count
left join visits_counts
on anc_open_count.month_start_date=visits_counts.month_start_date
left join volunteer_referrals
on anc_open_count.month_start_date=volunteer_referrals.month_start_date
