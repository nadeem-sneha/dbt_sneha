{{ config(
  materialized='table'
) }}

SELECT *
FROM {{ref('volunteer_case_normalized')}} 