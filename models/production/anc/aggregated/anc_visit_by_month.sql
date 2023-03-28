{{ config(
  materialized='table'
) }}


WITH visits_casedata AS (
  SELECT visits.*, c.clusterid,c.clustername,c.program_code,c.coid
  FROM {{ref('anc_visit_normalized')}} AS visits 
  LEFT JOIN {{ref('anc_case_normalized')}} AS c
  ON visits.caseid=c.id
  WHERE visits.visitreason='ANC')
  
SELECT clusterid,clustername,coid,program_code, date_trunc('month', visitdate ) AS visit_month,count(DISTINCT caseid) AS visits_count
FROM visits_casedata 
GROUP BY clusterid,clustername,coid,program_code,visit_month

