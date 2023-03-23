{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'
) }}

select 
(_airbyte_data ->> 'id') as id,
date(NULLIF(_airbyte_data -> 'form' ->> 'edd_final','')) as edddate,
(_airbyte_data -> 'form' ->> 'pregoutcome') as  pregoutcome,
(_airbyte_data -> 'form' ->> 'deliverysite') as delivery_site,
date(NULLIF(_airbyte_data -> 'form' ->> 'deliverydate','')) as delivery_date, 
(_airbyte_data -> 'form' ->> 'twins') as is_twins,
(_airbyte_data -> 'form' ->> 'birth_weight')::integer as birth_weight,
(_airbyte_data -> 'form' ->> 'birth_weight_twins')::integer as birth_weight_twins,
date(NULLIF(_airbyte_data -> 'form' ->> 'visitdate','')) as visitdate, 
_airbyte_data -> 'form' ->> 'load_person_case_id' as caseid,
_airbyte_ab_id,
_airbyte_emitted_at
from {{ source('commcare_anc', 'raw_anc_visit') }} 
where (_airbyte_data -> 'form' ->> 'visitreason') = 'Delivery_information' 
AND (_airbyte_data ->> 'archived')::boolean = false
/*remove test data */
AND ((_airbyte_data -> 'form' ->> 'womanname') NOT LIKE '%Demo%'
OR (_airbyte_data -> 'form' ->> 'womanname') NOT LIKE '%dummy%'
OR (_airbyte_data -> 'form' ->> 'womanname') NOT LIKE '%error%')
/* remove incorrect screened case data */
AND (_airbyte_data -> 'form' ->> 'load_person_case_id') NOT IN 
(select caseid from {{ref('incorrectly_screened_case_duplicates_removed')}})