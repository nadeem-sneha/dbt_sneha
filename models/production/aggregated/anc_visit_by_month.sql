{{ config(
  materialized='table'
) }}

SELECT clusterid,coid,center,areaconcat AS area, date_trunc('month', visitdate ) AS visit_month,count(DISTINCT caseid)
FROM {{ref('anc_visit_duplicates_removed')}}
GROUP BY clusterid,area,center,coid,visit_month