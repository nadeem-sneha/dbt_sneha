{{ config(
  materialized='table'
) }}

SELECT program_code, clustername, coid,trimester,
sum(CAST(case when  anc_reg='Yes' then 1 else 0 end AS FLOAT )) / CAST(count(1) AS FLOAT) AS pct_anc_registered,
sum( case when currentmonthvisitstatus='Visited' then 1 else 0 end ) AS current_month_visited,
sum( case when currentmonthvisitstatus='Visited' then 0 else 1 end ) AS current_month_pending,
sum( case when (currentmonthvisitstatus NOT LIKE 'Visited' and high_risk_preg LIKE'Yes') then 1 else 0 end ) AS current_month_high_risk_pending,
sum( case when days_from_last_visit >30 then 1 else 0 end ) AS last_30days_pending,
sum( case when  high_risk_preg LIKE 'Yes' then 1 else 0 end ) AS high_risk_count,
sum(CAST( case when currentmonthvisitstatus='Visited' then 1 else 0 end AS FLOAT )) / CAST(count(1) AS FLOAT) AS pct_current_month_visited,
sum(CAST(case when  high_risk_preg='Yes' then 1 else 0 end AS FLOAT )) / CAST(count(1) AS FLOAT)  AS pct_high_risk
from {{ref('anc_case')}}
GROUP BY program_code,clustername,coid,trimester
ORDER BY program_code,clustername,coid,trimester
