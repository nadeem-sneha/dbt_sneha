{{ config(
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]
) }}

with incorrectly_screened_cte as (select (_airbyte_data->'form'->'case_load_person1'->'case'->>'@case_id')  AS caseid,
(_airbyte_data->'form'->>'person_organization_id') AS person_organization_id,
_airbyte_ab_id,
_airbyte_emitted_at
from {{ source('commcare_common', 'raw_update_remove_member') }} 
where (_airbyte_data -> 'form' -> 'remove_member' ->> 'member_remove_reason')='Incorrectly_screened')


{{ dbt_utils.deduplicate(
    relation='incorrectly_screened_cte',
    partition_by='caseid',
    order_by='_airbyte_emitted_at desc',
   )
}}