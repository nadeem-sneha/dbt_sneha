{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'
) }}

select (_airbyte_data->'form'->'case_load_person1'->'case'->>'@case_id')  AS caseid,
(_airbyte_data->'form'->>'person_organization_id') AS person_organization_id,
_airbyte_ab_id,
_airbyte_emitted_at
from {{ source('commcare_anc', 'raw_update_remove_member') }} 
where (_airbyte_data -> 'form' -> 'remove_member' ->> 'member_remove_reason')='Incorrectly_screened'