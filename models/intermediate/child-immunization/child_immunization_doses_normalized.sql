{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

with case_cte as (select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data ->> 'id' as vaccine_case_id,
        _airbyte_data -> 'properties' ->> 'child_case_id' as case_id,
        _airbyte_data -> 'properties' ->> 'dose_name' as  dose_name,
        _airbyte_data -> 'properties' ->> 'dose_given' as  dose_given,
        _airbyte_data -> 'properties' ->> 'vaccine_time' as  vaccine_timing,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'dose_on_time_date','')) as  dose_on_time_date,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'dose_update_on','')) as  dose_update_on
from {{ source('commcare_common', 'raw_case') }}
where (_airbyte_data -> 'properties' ->> 'case_type') = 'vaccine_doses' )

{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='vaccine_case_id',
    order_by='_airbyte_emitted_at desc',
   )
}}