{{ config(
  materialized='view',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

with case_cte as (select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data ->> 'id' as id,
        _airbyte_data -> 'properties' ->> 'case_name' as case_name,
        _airbyte_data -> 'properties' ->> 'childname' as child_name,
        _airbyte_data -> 'properties' ->> 'beneficiary_program_id' as beneficiary_program_id,
    	  _airbyte_data -> 'properties' ->> 'cluster_id' as clusterid,
        _airbyte_data -> 'properties' ->> 'cluster_name' as clustername,
    	  _airbyte_data -> 'properties' ->> 'co_id' as coid,
        _airbyte_data -> 'properties' ->> 'program_code' as program_code,
        _airbyte_data -> 'properties' ->> 'program_name' as program_name,
        _airbyte_data -> 'properties' ->> 'hh_number' as hh_number,
        _airbyte_data -> 'properties' ->> 'aww_number' as aww_number,
    	(_airbyte_data ->  'closed')::boolean as closed,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'childdob','')) as child_dob,
        extract(year from age(current_date,date(NULLIF(_airbyte_data -> 'properties' ->> 'childdob','')))) * 12 + extract(month from age(current_date,date(NULLIF(_airbyte_data -> 'properties' ->> 'childdob','')))) AS age_in_months,
        _airbyte_data -> 'properties' ->> 'mother_case_id' as mother_case_id,
        _airbyte_data -> 'properties' ->> 'mother_name' as mother_name,
        _airbyte_data -> 'properties' ->> 'mother_program_id' as mother_program_id,
        _airbyte_data -> 'properties' ->> 'vaccine_completed' as vaccine_completed,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'min_followup_date','')) as min_followup_date,
        _airbyte_data -> 'properties' ->> 'case_type' as  case_type,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'date_opened','')) as  case_opened_date,
        _airbyte_data -> 'properties' ->> 'individual_category' as  individual_category,
        _airbyte_data -> 'properties' ->> 'service_registration' as  service_registration,
        date(_airbyte_data ->> 'date_closed'::text) as  date_closed
from {{ source('commcare_common', 'raw_case') }}

where (_airbyte_data -> 'properties' ->> 'case_type') = 'case' 
AND (_airbyte_data -> 'properties' ->> 'service_registration') = 'child'
/*removing test cases */
AND (_airbyte_data -> 'properties' ->> 'childname') NOT LIKE '%Demo%'
AND (_airbyte_data -> 'properties' ->> 'childname') NOT LIKE '%dummy%'
AND (_airbyte_data -> 'properties' ->> 'childname') NOT LIKE '%error%'
AND (_airbyte_data -> 'properties' ->> 'childname') NOT LIKE '%Demo%'
/* remove incorrect screened case data */
AND  (_airbyte_data ->> 'id') NOT IN 
(select caseid from {{ref('incorrectly_screened_case_normalized')}}))

{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='id',
    order_by='_airbyte_emitted_at desc',
   )
}}