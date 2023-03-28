{{ config(
  materialized='table'
) }}

WITH calendar AS (
SELECT d::date as month_start_date,(d + '1 month'::interval - '1 day'::interval )::date AS month_end_date
FROM generate_series('2022-01-01',  now(), '1 month'::interval)d
)

SELECT calendar.month_start_date, v.*, 
CASE
    WHEN ((v.case_opened_date <= calendar.month_end_date) 
        AND ((v.date_closed>calendar.month_end_date)OR (v.date_closed IS NULL)))
    THEN true
    ELSE false 
    END AS volunteer_open_status
FROM calendar
CROSS JOIN  
(SELECT * FROM {{ref('volunteer_case')}}) v
WHERE ((v.case_opened_date <= calendar.month_end_date) 
AND ((v.date_closed>calendar.month_end_date)OR (v.date_closed IS NULL)))