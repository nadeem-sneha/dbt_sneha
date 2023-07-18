{{ config(
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}

with reg_cte as (select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data ->> 'id' as id,
        _airbyte_data -> 'form' -> 'case_load_person0' -> 'case' ->> '@case_id' as caseid,
        _airbyte_data -> 'form' ->> 'womanname'  as womanname,
        date(NULLIF(_airbyte_data -> 'form' ->> 'registration_date','')) as anc_identify_date

from {{ source('commcare_anc', 'raw_anc_registration') }}
WHERE (_airbyte_data ->> 'archived')::boolean = false
AND ((_airbyte_data -> 'form' ->> 'womanname') NOT LIKE '%Demo%'
OR (_airbyte_data -> 'form' ->> 'womanname') NOT LIKE '%dummy%'
OR (_airbyte_data -> 'form' ->> 'womanname') NOT LIKE '%error%')
/* remove incorrect screened case data */
AND  (_airbyte_data -> 'form' -> 'case_load_person0' -> 'case' ->> '@case_id') NOT IN 
(select caseid from {{ref('incorrectly_screened_case_normalized')}}))

{{ dbt_utils.deduplicate(
    relation='reg_cte',
    partition_by='caseid',
    order_by='_airbyte_emitted_at desc',
   )
}}
