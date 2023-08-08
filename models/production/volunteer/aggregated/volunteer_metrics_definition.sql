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

--Volunteer open count(finding the total volunteer count)
volunteers_open AS 
(
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct case_id) as volunteer_open_count,
v.program_name,v.clustername,v.co_id,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT case_id, meeting_date, sex, program_name,clustername,co_id,aww_number 
FROM {{ref('volunteer_training')}}) v
WHERE (v.meeting_date BETWEEN calendar.month_start_date AND calendar.month_end_date)
OR (v.meeting_date IS NULL)
GROUP BY calendar.month_start_date,calendar.month_end_date,clustername,program_name,co_id,aww_number),


-- determine volunteer count for each time period for female and male

-- female volunteer count
volunteers_female AS 
(
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct case_id) AS female_volunteer_count,
v.program_name,v.clustername,v.co_id,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT case_id, meeting_date, sex, program_name,clustername,co_id,aww_number 
FROM {{ref('volunteer_training')}}) v

WHERE ((sex='female') 
AND (v.meeting_date BETWEEN calendar.month_start_date AND calendar.month_end_date)
OR (v.meeting_date IS NULL))
GROUP BY calendar.month_start_date,calendar.month_end_date, clustername,program_name,co_id,aww_number),

--male volunteer count
volunteers_male AS 
(
SELECT calendar.month_start_date, calendar.month_end_date, count(distinct case_id) AS male_volunteer_count,
v.program_name,v.clustername,v.co_id,v.aww_number
FROM calendar
CROSS JOIN  
(SELECT case_id, meeting_date, sex, program_name,clustername,co_id,aww_number 
FROM {{ref('volunteer_training')}}) v

WHERE ((sex='male') 
AND (v.meeting_date BETWEEN calendar.month_start_date AND calendar.month_end_date)
OR (v.meeting_date IS NULL))
GROUP BY calendar.month_start_date,calendar.month_end_date, clustername,program_name,co_id,aww_number)

--Final Table
select 
volunteers_open.month_start_date,
volunteers_open.month_end_date,
volunteers_open.program_name,
volunteers_open.clustername,
volunteers_open.co_id,
volunteers_open.aww_number,
volunteers_open.volunteer_open_count,
volunteers_female.female_volunteer_count,
volunteers_male.male_volunteer_count
from volunteers_open
left join volunteers_female
on volunteers_female.program_name = volunteers_open.program_name 
  and volunteers_female.clustername = volunteers_open.clustername
  and volunteers_female.co_id = volunteers_open.co_id 
  and volunteers_female.aww_number = volunteers_open.aww_number
  and volunteers_female.month_start_date = volunteers_open.month_start_date
  and volunteers_female.month_end_date = volunteers_open.month_end_date
left join volunteers_male
on volunteers_open.program_name = volunteers_male.program_name 
  and volunteers_open.clustername = volunteers_male.clustername
  and volunteers_open.co_id = volunteers_male.co_id 
  and volunteers_open.aww_number = volunteers_male.aww_number
  and volunteers_open.month_start_date = volunteers_male.month_start_date
  and volunteers_open.month_end_date = volunteers_male.month_end_date

order by program_name, month_start_date, month_end_date, clustername, co_id, aww_number