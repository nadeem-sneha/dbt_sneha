{{ config(
  materialized='table'
) }}

SELECT outcomes.*, 
CASE 
  WHEN outcomes.delivery_site = 'Home delivery' THEN 'Home'
  WHEN outcomes.delivery_site IS NOT NULL THEN 'Institutional'
END AS delivery_site_type,
CASE 
  WHEN (outcomes.birth_weight >= 2500 OR outcomes.birth_weight_twins >= 2500) THEN 'No'
  WHEN (outcomes.birth_weight < 2500 OR outcomes.birth_weight_twins < 2500) THEN 'Yes'
END AS low_birth_weight,
c.clusterid,c.clustername,c.program_code,c.coid,c.womanname,c.womanid,c.aww_number,c.hh_number
FROM {{ref('anc_outcome_duplicates_removed')}} AS outcomes 
LEFT JOIN {{ref('anc_case_duplicates_removed')}} AS c
ON outcomes.caseid=c.id