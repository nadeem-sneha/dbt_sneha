{{ config(
  materialized='table',
   indexes=[
    ],
    schema='intermediate'

) }}


with immunization_json_cte as (select 
    _airbyte_ab_id,
    _airbyte_emitted_at,
    _airbyte_data -> 'form' -> 'case_load_person0' ->'case'->>'@case_id' AS case_id, 
    json_array_elements((_airbyte_data -> 'form'  -> 'calculate_next_followup_info')::json) as immunization_json
from {{source('commcare_immunization','raw_child_immunization_due_doses')}}),

immunization_cte as (select 
    _airbyte_emitted_at,
    case_id,
    immunization_json -> 'check' ->> 'vaccine_dose_name' as dose_name,
    CASE WHEN 
        immunization_json -> 'check' ->> 'is_dose_administered' ='1' THEN 1
        ELSE 0
    END AS dose_given,
    date(NULLIF(immunization_json -> 'check' ->> 'date_eligible','')) as date_eligible,
    date(NULLIF(immunization_json -> 'check' ->> 'dose_followup_date','')) as dose_followup_date
from immunization_json_cte)

{{ dbt_utils.deduplicate(
    relation='immunization_cte',
    partition_by='case_id,dose_name',
    order_by='_airbyte_emitted_at desc',
   )
}}




