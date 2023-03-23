{{ config(
  materialized='table',
   indexes=[
      {'columns': ['caseid'], 'type': 'hash'}
    ],
    schema='intermediate'
) }}

{{ dbt_utils.deduplicate(
    relation=ref('incorrectly_screened_case'),
    partition_by='caseid',
    order_by='_airbyte_emitted_at desc',
   )
}}