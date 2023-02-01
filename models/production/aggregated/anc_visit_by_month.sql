{{ config(
  materialized='table'
) }}


WITH visits_casedata AS (
  SELECT visits.*, c.clusterid,c.clustername,c.program_code,c.coid
  FROM {{ref('anc_visit_duplicates_removed')}} AS visits 
  LEFT JOIN {{ref('anc_case_duplicates_removed')}} AS c
  ON visits.caseid=c.id)
  

SELECT clusterid,clustername,coid,program_code, conducted_by, date_trunc('month', visitdate ) AS visit_month,count(DISTINCT caseid)
FROM visits_casedata
GROUP BY clusterid,clustername,coid,program_code,conducted_by,visit_month

