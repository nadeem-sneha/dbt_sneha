

{{ config(
  materialized='table',
   indexes=[
      {'columns': ['id'], 'type': 'hash'}
    ],
    schema='intermediate'
) }}

{{ dbt_utils.deduplicate(
    relation=ref('anc_information'),
    partition_by='id',
    order_by='_airbyte_emitted_at desc',
   )
}}