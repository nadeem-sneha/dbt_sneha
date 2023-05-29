{{ config(
  materialized='table'
) }}

WITH calendar AS (
SELECT d::date as month_start_date,(d + '1 month'::interval - '1 day'::interval )::date AS month_end_date
FROM generate_series('2022-01-01',  now(), '1 month'::interval)d
)

SELECT c.id, calendar.month_start_date,c.program_name,c.program_code,c.clustername,c.coid,c.aww_number,c.lmpdate,c.anc_identify_date, c.anc_close_date,c.anc_reg_trimester,
CASE 
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=0 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=3  ) AND (calendar.month_start_date >= c.lmpdate) then 'First trimester'
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=4 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=6  ) AND (calendar.month_start_date >= c.lmpdate) then 'Second trimester'
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>=7 AND date_part('months',age(calendar.month_end_date, c.lmpdate))<=10 ) AND (calendar.month_start_date >= c.lmpdate) then 'Third trimester'
  when (date_part('months',age(calendar.month_end_date, c.lmpdate))>10 ) AND (calendar.month_start_date >= c.lmpdate) then 'Over-due'
  ELSE 'NA'
END AS trimester, 
CASE 
  WHEN ((c.anc_identify_date <= calendar.month_end_date) AND
((c.anc_close_date>=calendar.month_start_date)OR (c.anc_close_date IS NULL)))
  THEN true
  ELSE false 
  END AS open_status,
CASE 
  WHEN ((c.anc_identify_date <= calendar.month_end_date) AND
((c.anc_close_date>calendar.month_end_date)OR (c.anc_close_date IS NULL)) AND (c.earliest_hb_grade_date <= calendar.month_end_date))
  THEN true
  ELSE false 
  END AS anemia_tested_status
FROM calendar
CROSS JOIN  
(SELECT  id,program_name,program_code,clustername,coid,aww_number, anc_identify_date,anc_close_date,anc_closed,lmpdate,earliest_hb_grade_date,anc_reg_trimester FROM {{ref('anc_case')}}) c 

GROUP BY calendar.month_start_date,month_end_date,anc_identify_date,anc_close_date, lmpdate, id,program_name,program_code,clustername,coid,aww_number,anc_reg_trimester,earliest_hb_grade_date
