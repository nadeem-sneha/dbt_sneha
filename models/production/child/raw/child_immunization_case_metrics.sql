{{ config(
  materialized='table'
) }}

SELECT * FROM {{ref('child_case')}}
WHERE ((age_in_months BETWEEN 13 AND 24) AND closed IS FALSE AND  program_code != 'MMP')