{{ config(
  materialized='table'
) }}

with immunizations_all_combinations_cte as (
    SELECT a.id,b.dose_name from 
        ((select distinct id from {{ref('child_case_normalized')}})a
            cross join
        (select distinct dose_name from {{ref('child_immunization_normalized')}} where dose_name !='')b)
),

immunizations_cte as (
SELECT 
    i.id,
    c.vaccine_case_id,
    i.dose_name,
    c.dose_given,
    c.vaccine_timing,
    c.dose_on_time_date,
    c.dose_update_on
FROM {{ ref('child_immunization_normalized') }} AS c
RIGHT JOIN immunizations_all_combinations_cte AS i
ON c.child_case_id = i.id 
and c.dose_name=i.dose_name
)

SELECT c.clusterid,c.clustername,c.program_code,c.coid,c.aww_number,c.hh_number,c.child_name,c.mother_name,c.child_dob, c.age_in_months, immunizations_cte.* 
FROM immunizations_cte
LEFT JOIN {{ref('child_case_normalized')}} AS c
ON immunizations_cte.id=c.id
-- ignore MMP as it does not seem to have an immunization component
WHERE c.program_code != 'MMP' AND c.age_in_months <=24