{{ config(
  materialized='table'
) }}

SELECT visits.*, date_trunc('month',visits.visitdate) AS month_start_date,
c.clusterid,c.clustername,c.program_code,c.coid,c.womanname,c.womanid,c.aww_number,c.hh_number
FROM {{ref('anc_visit_normalized')}} AS visits  
LEFT JOIN {{ref('anc_case_normalized')}} AS c
ON visits.caseid=c.id
WHERE  (visits.visitreason='ANC' or visits.visitreason='Close_case')