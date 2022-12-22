{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}



select _airbyte_data -> 'visitdate' as visitdate, _airbyte_data -> 'case_@case_id' as caseid, _airbyte_ab_id from {{ source('source_commcare', 'anc') }} 
