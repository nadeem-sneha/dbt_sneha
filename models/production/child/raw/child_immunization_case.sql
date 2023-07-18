{{ config(
  materialized='table'
) }}

SELECT * FROM {{ref('child_case')}}
WHERE ((age_in_months BETWEEN 0 AND 24) AND closed IS FALSE AND  program_code != 'MMP')