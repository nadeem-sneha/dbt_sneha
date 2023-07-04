{{ config(
  materialized='table'
) }}

with immunizations_latest_critical_cte as (
     SELECT id, max(dose_update_on) as vaccine_completed_on from {{ref('child_immunization_status')}}
     where dose_name IN ('BCG' , 'Polio-1', 'IPV-1' ,'Polio-2','Polio-3','IPV-2','Penta-1','Penta-2','Penta -3','MR-1','MR-2')
     GROUP BY id
),

immunizations_latest_cte as (
    SELECT id, max(dose_update_on) as dose_update_on from {{ref('child_immunization_status')}}
    GROUP BY id
)

SELECT  c.*,
        immunizations_latest_cte.dose_update_on,
        case when vaccine_completed='yes'
            then immunizations_latest_critical_cte.vaccine_completed_on
            else NULL
        END AS vaccine_completed_on
FROM {{ref('child_case_normalized')}} AS c
LEFT JOIN
immunizations_latest_cte
ON c.id=immunizations_latest_cte.id
LEFT JOIN
immunizations_latest_critical_cte
ON c.id=immunizations_latest_critical_cte.id