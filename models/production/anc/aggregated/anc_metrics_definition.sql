{{ config(
  materialized='table',
  indexes=[
      {'columns': ['month_start_date']}
    ],
) }}

-- create  calendar with all possible start and end dates
WITH calendar AS (
SELECT a.month_start_date,b.month_end_date from 
(SELECT d::date as month_start_date,(d + '1 month'::interval - '1 day'::interval )::date AS month_end_date FROM generate_series('2022-01-01',  now(), '1 month'::interval)d)a 
CROSS JOIN 
(SELECT d::date as month_start_date,(d + '1 month'::interval - '1 day'::interval )::date AS month_end_date FROM generate_series('2022-01-01',  now(), '1 month'::interval)d)b 
where a.month_start_date < b.month_end_date
),

-- determine anc open count for each time period

anc_open_cases as (SELECT c.id, calendar.month_start_date,calendar.month_end_date, c.program_name,c.program_code,c.clustername,c.coid,c.aww_number,c.lmpdate,c.anc_identify_date, c.anc_close_date,c.anc_reg_trimester,
CASE 
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=0 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=3  ) AND (calendar.month_start_date >= c.lmpdate) then 'First trimester'
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=4 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=6  ) AND (calendar.month_start_date >= c.lmpdate) then 'Second trimester'
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=7 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=10 ) AND (calendar.month_start_date >= c.lmpdate) then 'Third trimester'
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>10 ) AND (calendar.month_start_date >= c.lmpdate) then 'Over-due'
  ELSE 'NA'
END AS trimester, 
CASE 
  WHEN (
  (((calendar.month_end_date-calendar.month_start_date)::int <31) and 
  (c.anc_identify_date <= calendar.month_end_date) AND
  ((c.anc_close_date>calendar.month_end_date)OR (c.anc_close_date IS NULL)))
  OR 
  (((calendar.month_end_date-calendar.month_start_date)::int >= 31) and 
  (c.anc_identify_date <= calendar.month_end_date) AND
  ((c.anc_close_date>(calendar.month_start_date+30))OR (c.anc_close_date IS NULL)))
  )
  THEN true
  ELSE false 
  END AS open_status,
CASE 
  WHEN (
  (((calendar.month_end_date-calendar.month_start_date)::int <31) and 
  (c.anc_identify_date <= calendar.month_end_date) AND
  ((c.anc_close_date>calendar.month_end_date)OR (c.anc_close_date IS NULL)) AND
  (c.earliest_hb_grade_date <= calendar.month_end_date))
  OR 
  (((calendar.month_end_date-calendar.month_start_date)::int >= 31) and 
  (c.anc_identify_date <= calendar.month_end_date) AND
  ((c.anc_close_date>(calendar.month_start_date+30))OR (c.anc_close_date IS NULL)) AND 
  (c.earliest_hb_grade_date <= calendar.month_end_date))
  )
  THEN true
  ELSE false 
  END AS anemia_tested_status
FROM calendar
CROSS JOIN  
(SELECT  id,program_name,program_code,clustername,coid,aww_number, anc_identify_date,anc_close_date,anc_closed,lmpdate,earliest_hb_grade_date,anc_reg_trimester FROM {{ref('anc_case')}}) c 
GROUP BY calendar.month_start_date,month_end_date,anc_identify_date,anc_close_date, lmpdate, id,program_name,program_code,clustername,coid,aww_number,anc_reg_trimester,earliest_hb_grade_date),


anc_open_count AS (SELECT calendar.month_start_date, calendar.month_end_date, 
count(distinct c.id) AS open_case_count,
sum(case when c.anemia_tested_status is TRUE then 1 else 0 END) AS anemia_tested_count,
sum(case when c.anc_reg_trimester = 'First_trim' then 1 else 0 END) AS early_registration_count,
sum(case when c.anc_reg_trimester = 'First_trim' then 1 else 0 END) AS anc_reg_first_trimester_count,
sum(case when c.anc_reg_trimester = 'Second_trim' then 1 else 0 END) AS anc_reg_second_trimester_count,
sum(case when c.anc_reg_trimester = 'Third_trim' then 1 else 0 END) AS anc_reg_third_trimester_count,
c.program_code,c.clustername,c.coid,c.aww_number
FROM calendar
LEFT JOIN  
(SELECT id,month_start_date, month_end_date,open_status,anemia_tested_status,anc_reg_trimester,  program_code,clustername,coid,aww_number 
FROM anc_open_cases) c 
ON calendar.month_start_date=c.month_start_date and calendar.month_end_date=c.month_end_date
where c.open_status is TRUE
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number ),


-- total unique visits 
visits_counts AS (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.caseid) AS total_visits_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, visitreason, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number ),

-- co unique visits 
visits_counts_co AS (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.caseid) AS co_visits_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, visitreason, conducted_by, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND conducted_by='co' AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),

-- volunteer unique visits 
visits_counts_volunteer AS (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.caseid) AS volunteer_visits_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, visitreason, conducted_by, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND conducted_by='volunteer' AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),

-- volunteer count
-- determine volunteer open count for each time period

volunteers_open AS 
(
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct id) AS volunteer_open_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT id, date_closed, case_opened_date, sex, program_code,clustername,coid,aww_number 
FROM {{ref('volunteer_case')}}) v
WHERE ((sex='female') AND 
((v.case_opened_date <= calendar.month_end_date) 
AND ((v.date_closed>calendar.month_start_date)OR (v.date_closed IS NULL))))
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),

-- unique volunteers with referrals volunteers_involved_in_anc_referral_count
volunteers_with_referrals as (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.volunteerid) AS volunteers_involved_in_anc_referral_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT volunteerid,referral, visitdate, visitreason, conducted_by, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND referral='Yes' AND conducted_by ='volunteer' AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),

-- co visits with referrals
co_referrals AS (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.caseid) AS co_referral_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, referral, visitreason, conducted_by, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND referral='Yes' AND conducted_by='co' AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),

-- volunteer visits with referrals
volunteer_referrals AS (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.caseid) AS volunteer_referral_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, referral, visitreason, conducted_by, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND referral='Yes' AND conducted_by='volunteer' AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),

-- visits with THR referrals
thr_referrals AS (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.caseid) AS thr_referral_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, referral, visitreason, referral_reasons, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND referral='Yes' AND (referral_reasons LIKE '%ICDS_THR%' ) AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),

--  visits with THR
thr_visits AS (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct v.caseid) AS thr_count,
v.program_code,v.clustername,v.coid,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, ancthr, visitreason, program_code,clustername,coid,aww_number 
FROM {{ref('anc_visit')}}) v
WHERE visitreason='ANC' AND ancthr ='Yes' AND v.visitdate  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number),



-- institutional deliveries
deliveries as (
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct o.caseid) AS delivery_count,
sum(case when o.delivery_site_type='Institutional' THEN 1 ELSE 0 END) AS institutional_delivery_count,
o.program_code,o.clustername,o.coid,o.aww_number
FROM calendar
CROSS JOIN  
(SELECT caseid,visitdate, delivery_date, pregoutcome, delivery_site_type, program_code,clustername,coid,aww_number 
FROM {{ref('anc_outcome')}}) o
WHERE o.pregoutcome IN ('livebirth','stillbirth') AND o.delivery_date  BETWEEN calendar.month_start_date AND calendar.month_end_date
GROUP BY calendar.month_start_date,calendar.month_end_date,program_code,clustername,coid,aww_number)



select 
anc_open_count.month_start_date,
anc_open_count.month_end_date,
anc_open_count.program_code,
anc_open_count.clustername,
anc_open_count.coid,
anc_open_count.aww_number,
anc_open_count.open_case_count,
anc_open_count.anemia_tested_count,
anc_open_count.early_registration_count,
anc_open_count.anc_reg_first_trimester_count,
anc_open_count.anc_reg_second_trimester_count,
anc_open_count.anc_reg_third_trimester_count,
visits_counts.total_visits_count,
visits_counts_co.co_visits_count, 
visits_counts_volunteer.volunteer_visits_count,
volunteers_open.volunteer_open_count,
volunteers_with_referrals.volunteers_involved_in_anc_referral_count,
co_referrals.co_referral_count,
volunteer_referrals.volunteer_referral_count,
thr_referrals.thr_referral_count,
thr_visits.thr_count,
deliveries.delivery_count,
deliveries.institutional_delivery_count
from anc_open_count

left join visits_counts
on anc_open_count.month_start_date=visits_counts.month_start_date and 
anc_open_count.month_end_date=visits_counts.month_end_date and 
anc_open_count.program_code=visits_counts.program_code and
anc_open_count.clustername=visits_counts.clustername and
anc_open_count.coid=visits_counts.coid and
anc_open_count.aww_number=visits_counts.aww_number

left join visits_counts_volunteer
on anc_open_count.month_start_date=visits_counts_volunteer.month_start_date and 
anc_open_count.month_end_date=visits_counts_volunteer.month_end_date and 
anc_open_count.program_code=visits_counts_volunteer.program_code and
anc_open_count.clustername=visits_counts_volunteer.clustername and
anc_open_count.coid=visits_counts_volunteer.coid and
anc_open_count.aww_number=visits_counts_volunteer.aww_number

left join visits_counts_co
on anc_open_count.month_start_date=visits_counts_co.month_start_date and 
anc_open_count.month_end_date=visits_counts_co.month_end_date and
anc_open_count.program_code=visits_counts_co.program_code and
anc_open_count.clustername=visits_counts_co.clustername and
anc_open_count.coid=visits_counts_co.coid and
anc_open_count.aww_number=visits_counts_co.aww_number

left join volunteers_open
on anc_open_count.month_start_date = volunteers_open.month_start_date and 
anc_open_count.month_end_date=volunteers_open.month_end_date and
anc_open_count.program_code=volunteers_open.program_code and
anc_open_count.clustername=volunteers_open.clustername and
anc_open_count.coid=volunteers_open.coid and
anc_open_count.aww_number=volunteers_open.aww_number

left join volunteers_with_referrals
on anc_open_count.month_start_date=volunteers_with_referrals.month_start_date and 
anc_open_count.month_end_date=volunteers_with_referrals.month_end_date and
anc_open_count.program_code=volunteers_with_referrals.program_code and
anc_open_count.clustername=volunteers_with_referrals.clustername and
anc_open_count.coid=volunteers_with_referrals.coid and
anc_open_count.aww_number=volunteers_with_referrals.aww_number

left join co_referrals
on anc_open_count.month_start_date=co_referrals.month_start_date and 
anc_open_count.month_end_date=co_referrals.month_end_date and
anc_open_count.program_code=co_referrals.program_code and
anc_open_count.clustername=co_referrals.clustername and
anc_open_count.coid=co_referrals.coid and
anc_open_count.aww_number=co_referrals.aww_number

left join volunteer_referrals
on anc_open_count.month_start_date=volunteer_referrals.month_start_date and 
anc_open_count.month_end_date=volunteer_referrals.month_end_date and
anc_open_count.program_code=volunteer_referrals.program_code and
anc_open_count.clustername=volunteer_referrals.clustername and
anc_open_count.coid=volunteer_referrals.coid and
anc_open_count.aww_number=volunteer_referrals.aww_number

left join thr_referrals
on anc_open_count.month_start_date=thr_referrals.month_start_date and 
anc_open_count.month_end_date=thr_referrals.month_end_date and
anc_open_count.program_code=thr_referrals.program_code and
anc_open_count.clustername=thr_referrals.clustername and
anc_open_count.coid=thr_referrals.coid and
anc_open_count.aww_number=thr_referrals.aww_number

left join thr_visits
on anc_open_count.month_start_date=thr_visits.month_start_date and 
anc_open_count.month_end_date=thr_visits.month_end_date and
anc_open_count.program_code=thr_visits.program_code and
anc_open_count.clustername=thr_visits.clustername and
anc_open_count.coid=thr_visits.coid and
anc_open_count.aww_number=thr_visits.aww_number

left join deliveries
on anc_open_count.month_start_date=deliveries.month_start_date and 
anc_open_count.month_end_date=deliveries.month_end_date and
anc_open_count.program_code=deliveries.program_code and
anc_open_count.clustername=deliveries.clustername and
anc_open_count.coid=deliveries.coid and
anc_open_count.aww_number=deliveries.aww_number

order by program_code, month_start_date,month_end_date, clustername, coid,aww_number

