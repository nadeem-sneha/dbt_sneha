{{ config(
  materialized='table'
) }}

with outcomes_cte as (SELECT outcomes.*, date_trunc('month',outcomes.delivery_date) AS delivery_month,
CASE 
  WHEN outcomes.delivery_site = 'Home delivery' THEN 'Home'
  WHEN outcomes.delivery_site IS NOT NULL THEN 'Institutional'
END AS delivery_site_type,
CASE 
  WHEN (outcomes.birth_weight >= 2500 OR outcomes.birth_weight_twins >= 2500) THEN 'No'
  WHEN (outcomes.birth_weight < 2500 OR outcomes.birth_weight_twins < 2500) THEN 'Yes'
END AS low_birth_weight,
c.clusterid,c.clustername,c.program_code,c.coid,c.womanname,c.womanid,c.aww_number,c.hh_number
FROM {{ref('anc_outcome_normalized')}} AS outcomes 
LEFT JOIN {{ref('anc_case_normalized')}} AS c
ON outcomes.caseid=c.id)

-- removing multiple CO entries due to commcare code bug assuming they are entered in same month
{{ dbt_utils.deduplicate(
    relation='outcomes_cte',
    partition_by='caseid,delivery_month',
    order_by='delivery_date desc',
   )
}}

