{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'
) }}

with vol_cte as (select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data ->> 'id' as id,
        _airbyte_data ->'form'->'case_load_case0'->'case'->>'@case_id' AS case_id,
        date(NULLIF(_airbyte_data -> 'form' ->> 'meetingdate','')) as meeting_date,
        _airbyte_data -> 'form' ->> 'meetingtypes' as meeting_type,
        _airbyte_data -> 'form' ->> 'voltrain' as  voltrain,
        _airbyte_data -> 'form' ->> 'interaction' as interaction,
    	  _airbyte_data -> 'form' ->> 'vol_enrolldate' as vol_enrolldate

from {{ source('commcare_volunteer', 'raw_volunteer_training') }}

where (_airbyte_data ->> 'archived')::boolean = false
/*removing test cases */
AND (_airbyte_data -> 'form' ->> 'person_name') NOT LIKE '%Demo%'
AND (_airbyte_data -> 'form' ->> 'person_name') NOT LIKE '%dummy%'
AND (_airbyte_data -> 'form' ->> 'person_name') NOT LIKE '%error%'
/* remove incorrect screened case data */
AND  (_airbyte_data ->'form'->'case_load_case0'->'case'->>'@case_id') NOT IN (select caseid from {{ref('incorrectly_screened_case_normalized')}})
)

{{ dbt_utils.deduplicate(
    relation='vol_cte',
    partition_by='id',
    order_by='_airbyte_emitted_at desc',
   )
}}