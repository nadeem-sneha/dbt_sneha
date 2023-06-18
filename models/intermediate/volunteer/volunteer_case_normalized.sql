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
    	  _airbyte_data -> 'properties' ->> 'co_id' as co_id_from_case,
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
AND (_airbyte_data -> 'properties' ->> 'service_registration') = 'volunteer'
/*removing test cases */
AND (_airbyte_data -> 'properties' ->> 'person_name') NOT LIKE '%Demo%'
AND (_airbyte_data -> 'properties' ->> 'person_name') NOT LIKE '%dummy%'
AND (_airbyte_data -> 'properties' ->> 'person_name') NOT LIKE '%error%'
AND (_airbyte_data -> 'properties' ->> 'cluster_name') NOT LIKE '%Demo%'
/* remove incorrect screened case data */
AND  (_airbyte_data ->> 'id') NOT IN 
(select caseid from {{ref('incorrectly_screened_case_normalized')}})),

/* Using volunteer info data to get co_id instead of from the case file due to softwar ebug related to 
load and save of this data into case files*/

vol_info_cte AS 
(SELECT 
  _airbyte_data ->'form'->'case_load_person0'->'case'->>'@case_id' AS case_id,
  _airbyte_data ->'form'->> 'co_id' AS co_id
  FROM {{ source('commcare_common', 'raw_volunteer_info')}}
),

vol_complete_cte as (
  SELECT v1.*,
  COALESCE(v2.co_id,v1.co_id_from_case) AS coid FROM vol_cte AS v1 
  LEFT JOIN vol_info_cte AS v2
  ON v1.id = v2.case_id
)

{{ dbt_utils.deduplicate(
    relation='vol_complete_cte',
    partition_by='id',
    order_by='_airbyte_emitted_at desc',
   )
}}