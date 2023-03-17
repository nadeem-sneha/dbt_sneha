{{ config(
  materialized='table',
   indexes=[
      {'columns': ['id'], 'type': 'hash'}
    ],
    schema='intermediate'
) }}

{{ dbt_utils.deduplicate(
    relation=ref('anc_registration_normalized'),
    partition_by='caseid',
    order_by='_airbyte_emitted_at desc',
   )
}}