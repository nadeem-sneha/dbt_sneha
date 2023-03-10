{{ config(
  materialized='table'
) }}

WITH calendar AS (
SELECT d::date as month_start_date,(d + '1 month'::interval - '1 day'::interval )::date AS month_end_date
FROM generate_series('2022-01-01',  now(), '1 month'::interval)d
)

SELECT calendar.month_start_date, count(c.id) AS anc_open_count,c.clustername FROM calendar
CROSS JOIN  
(SELECT  id,clustername, anc_identify_date,anc_close_date FROM {{ref('anc_case')}}) c 
WHERE (c.anc_identify_date <= calendar.month_end_date AND c.anc_close_date IS NULL)
OR (c.anc_identify_date <= calendar.month_end_date AND c.anc_close_date >= calendar.month_start_date)
GROUP BY calendar.month_start_date,c.clustername
ORDER BY calendar.month_start_date,c.clustername