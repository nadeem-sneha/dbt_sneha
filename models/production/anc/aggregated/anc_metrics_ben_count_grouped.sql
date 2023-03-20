{{ config(
  materialized='table'
) }}

WITH calendar AS (
SELECT d::date as month_start_date,(d + '1 month'::interval - '1 day'::interval )::date AS month_end_date
FROM generate_series('2022-01-01',  now(), '1 month'::interval)d
)

SELECT calendar.month_start_date, c.program_name,c.clustername,c.coid,c.aww_number , count(c.id) AS anc_open_count,
CASE 
              when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=0 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=3  )then 'First trimester'
              when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=4 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=6  )then 'Second trimester'
              when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=7 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=10 ) then 'Third trimester'
              when (date_part('months',age(calendar.month_end_date, c.lmpdate))>10 ) then 'Over-due'
              ELSE 'NA'
END AS trimester
FROM calendar
CROSS JOIN  
(SELECT  id,program_name,clustername,coid,aww_number, anc_identify_date,anc_close_date,anc_closed,lmpdate FROM {{ref('anc_case')}}) c 
WHERE (c.anc_identify_date <= calendar.month_end_date AND c.anc_close_date IS NULL)
OR (c.anc_identify_date <= calendar.month_end_date AND c.anc_close_date >= calendar.month_start_date)
GROUP BY calendar.month_start_date,program_name,c.clustername,c.coid,c.aww_number,trimester
ORDER BY calendar.month_start_date,program_name,c.clustername,c.coid,c.aww_number,trimester