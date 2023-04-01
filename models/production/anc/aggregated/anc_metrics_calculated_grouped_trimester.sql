{{ config(
  materialized='table'
) }}

select * 
from {{ metrics.calculate(
    [metric('open_case_count')],
    grain='month',
    dimensions=['program_code','clustername','coid','aww_number','trimester']
) }}
