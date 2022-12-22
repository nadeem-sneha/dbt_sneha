{{ config(
  materialized='table'
) }}


SELECT clusterid,coid,center,areaconcat AS area, date_trunc('month', visitdate ) AS visit_month,count(DISTINCT caseid)
FROM {{ref('anc_information')}}
Group by clusterid,area,center,coid,visit_month