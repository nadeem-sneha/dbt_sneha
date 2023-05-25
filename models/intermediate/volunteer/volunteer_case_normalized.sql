{{ config(
  materialized='view',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

with vol_cte as (select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data ->> 'id' as id,
        _airbyte_data -> 'properties' ->> 'person_organization_id' as person_organizaton_id,
        _airbyte_data -> 'properties' ->> 'person_name' as person_name,
        _airbyte_data -> 'properties' ->> 'age' as  age,
        _airbyte_data -> 'properties' ->> 'sex' as  sex,
    	  _airbyte_data -> 'properties' ->> 'cluster_id' as clusterid,
        _airbyte_data -> 'properties' ->> 'cluster_name' as clustername,
    	  _airbyte_data -> 'properties' ->> 'co_id' as coid,
        _airbyte_data -> 'properties' ->> 'program_code' as program_code,
        _airbyte_data -> 'properties' ->> 'program_name' as program_name,
        _airbyte_data -> 'properties' ->> 'hh_number' as hh_number,
        _airbyte_data -> 'properties' ->> 'aww_number' as aww_number,
    	  (_airbyte_data ->  'closed')::boolean as closed,
        _airbyte_data -> 'properties' ->> 'case_type' as  case_type,
        _airbyte_data -> 'properties' ->> 'individual_category' as  individual_category,
        _airbyte_data -> 'properties' ->> 'service_registration' as  service_registration,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'date_opened','')) as  case_opened_date,
        date(_airbyte_data ->> 'date_closed'::text) as  date_closed
from {{ source('commcare_common', 'raw_case') }}
where (_airbyte_data -> 'properties' ->> 'case_type') = 'case' 
AND (_airbyte_data -> 'properties' ->> 'individual_category') = 'volunteer'
/*removing test cases */
AND (_airbyte_data -> 'properties' ->> 'person_name') NOT LIKE '%Demo%'
AND (_airbyte_data -> 'properties' ->> 'person_name') NOT LIKE '%dummy%'
AND (_airbyte_data -> 'properties' ->> 'person_name') NOT LIKE '%error%'
/* remove incorrect screened case data */
AND  (_airbyte_data ->> 'id') NOT IN 
(select caseid from {{ref('incorrectly_screened_case_normalized')}}))

{{ dbt_utils.deduplicate(
    relation='vol_cte',
    partition_by='id',
    order_by='_airbyte_emitted_at desc',
   )
}}