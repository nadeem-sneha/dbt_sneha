{{ config(
  materialized='table'
) }}

SELECT *,month_start_date as date_month from {{ref('anc_metrics')}}
where (month_end_date-month_start_date)::int < 31
