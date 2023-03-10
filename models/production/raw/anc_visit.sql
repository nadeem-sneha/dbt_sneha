{{ config(
  materialized='table'
) }}

SELECT visits.*, 
c.clusterid,c.clustername,c.program_code,c.coid,c.womanname,c.womanid,c.aww_number,c.hh_number
FROM {{ref('anc_visit_duplicates_removed')}} AS visits  
LEFT JOIN {{ref('anc_case_duplicates_removed')}} AS c
ON visits.caseid=c.id
WHERE  visits.visitreason='ANC' 