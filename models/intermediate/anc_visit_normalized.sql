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
(_airbyte_data ->> 'id') as id,
(_airbyte_data ->> 'womanid') as womanid,
(_airbyte_data ->> 'why_high_risk') as why_high_risk,
date(NULLIF(_airbyte_data ->> 'visitdate','')) as visitdate, 
_airbyte_data ->> 'load_person_case_id' as caseid, _airbyte_ab_id, _airbyte_emitted_at
from {{ source('commcare_anc', 'raw_anc_visit') }} 
