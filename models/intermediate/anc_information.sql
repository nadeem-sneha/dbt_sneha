{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}



select (_airbyte_data ->> 'visitreason') as visitreason, 
(_airbyte_data ->> 'clusterid') as clusterid,
(_airbyte_data ->> 'coid') as coid,
(_airbyte_data ->> 'center') as center,
(_airbyte_data ->> 'areaconcat') as areaconcat,
(_airbyte_data ->> 'id') as id,
date(_airbyte_data ->> 'visitdate') as visitdate, 
_airbyte_data ->> 'case_@case_id' as caseid, _airbyte_ab_id 
from {{ source('source_commcare', 'anc') }} 
