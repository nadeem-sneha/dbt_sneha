{{ config(
  materialized='table'
) }}

with immunizations_done as (
SELECT id, string_agg(dose_name, ', ') AS vaccinated_doses
FROM   {{ref('child_immunization_status')}}
where dose_given ='yes'
GROUP  BY id
),

immunizations_due as (
SELECT id, string_agg(dose_name, ', ') AS due_doses
FROM   {{ref('child_immunization_status')}}
where dose_given IS NULL
GROUP  BY id
),

immunizations_latest_critical_cte as (
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
        END AS vaccine_completed_on,
        immunizations_done.vaccinated_doses,
        immunizations_due.due_doses,
        CASE WHEN 
        ((immunizations_done.vaccinated_doses LIKE '%BCG%') AND
        ((immunizations_done.vaccinated_doses LIKE '%Polio-1%') OR (immunizations_done.vaccinated_doses LIKE '%IPV-1%')) AND
        (immunizations_done.vaccinated_doses LIKE '%Polio-2%') AND
        ((immunizations_done.vaccinated_doses LIKE '%Polio-3%')  OR (immunizations_done.vaccinated_doses LIKE '%IPV-2%'))AND
        (immunizations_done.vaccinated_doses LIKE '%Penta-1%') AND
        (immunizations_done.vaccinated_doses LIKE '%Penta-2%') AND
        (immunizations_done.vaccinated_doses LIKE '%Penta -3%') AND
        ((immunizations_done.vaccinated_doses LIKE '%MR-1%') OR (immunizations_done.vaccinated_doses LIKE '%MR-2%'))) 
        THEN 'yes' ELSE 'no' END as vaccine_completed_calculated
FROM {{ref('child_case_normalized')}} AS c
LEFT JOIN
immunizations_latest_cte
ON c.id=immunizations_latest_cte.id
LEFT JOIN
immunizations_latest_critical_cte
ON c.id=immunizations_latest_critical_cte.id
LEFT JOIN
immunizations_done
ON c.id = immunizations_done.id
LEFT JOIN
immunizations_due
ON c.id = immunizations_due.id