{{ config(
  materialized='table'
) }}

SELECT * FROM {{ref('child_case')}}
WHERE age_in_months <= 24 AND closed IS FALSE