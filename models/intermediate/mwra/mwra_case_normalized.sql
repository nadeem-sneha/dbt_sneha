{{
    config(
        indexes=[{"columns": ["_airbyte_ab_id"], "type": "hash"}],
    )
}}

with
    case_cte as (
        select
            _airbyte_ab_id,
            _airbyte_emitted_at,
            _airbyte_data ->> 'id' as case_id,                      
            _airbyte_data -> 'properties' ->> 'womanid' as woman_id,        
            _airbyte_data -> 'properties' ->> 'person_name' as person_name,
            _airbyte_data -> 'properties' ->> 'age' as woman_age,
            _airbyte_data -> 'properties' ->> 'hh_number' as hh_number,
            _airbyte_data -> 'properties' ->> 'aww_number' as aww_number,
            _airbyte_data -> 'properties' ->> 'person_organization_id' as person_organizaton_id,
            _airbyte_data -> 'properties' ->> 'case_name' as case_name,        -- use as PRIMARY KEY for referral-follow-up table
            _airbyte_data -> 'properties' ->> 'cluster_id' as clusterid,
            _airbyte_data -> 'properties' ->> 'cluster_name' as clustername,
            _airbyte_data -> 'properties' ->> 'co_id' as coid,
            _airbyte_data -> 'properties' ->> 'program_code' as program_code,
            _airbyte_data -> 'properties' ->> 'program_name' as program_name,
            (_airbyte_data -> 'closed')::boolean as closed,
            _airbyte_data -> 'properties' ->> 'case_type' as case_type,
            date(
                nullif(_airbyte_data -> 'properties' ->> 'date_opened', '')
            ) as case_opened_date,
            (_airbyte_data ->> 'date_closed')::date as case_closed_date,
            _airbyte_data -> 'properties'->> 'individual_category' as individual_category,
            _airbyte_data -> 'properties'->> 'service_registration' as service_registration
        from {{ source("commcare_common", "raw_case") }}

        where
            (_airbyte_data -> 'properties' ->> 'case_type') = 'case'
            and (_airbyte_data -> 'properties' ->> 'service_registration') = 'mwra'
            /* remove test cases */
            and (_airbyte_data -> 'properties' ->> 'person_name') not like '%Demo%'
            and (_airbyte_data -> 'properties' ->> 'person_name') not like '%dummy%'
            and (_airbyte_data -> 'properties' ->> 'person_name') not like '%error%'
            /* remove incorrect screened case data */
            and (_airbyte_data ->> 'id') not in (
                select caseid from {{ ref("incorrectly_screened_case_normalized") }}
            )
    )


{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='case_id',
    order_by='_airbyte_emitted_at desc',
   )
}}