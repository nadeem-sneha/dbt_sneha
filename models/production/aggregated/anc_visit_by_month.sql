{{ config(
  materialized='table'
) }}

SELECT clusterid,coid, conducted_by, date_trunc('month', visitdate ) AS visit_month,count(DISTINCT caseid)
FROM {{ref('anc_visit_duplicates_removed')}}
GROUP BY clusterid,coid,conducted_by,visit_month