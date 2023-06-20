{{
    config(
        materialized="table",
        indexes=[{"columns": ["_airbyte_ab_id"], "type": "hash"}],
    )
}}

with
    referral_cte as (
        select
            _airbyte_data, -- remove this
            _airbyte_data ->> 'id' as referral_followup_id,                          -- PRIMARY KEY
            -- _airbyte_data ->> 'case_id' as case_id,
            _airbyte_data -> 'properties' ->> 'case_name' as case_name,     -- FOREIGN KEY

            _airbyte_data -> 'properties' ->> 'womanid' as woman_id,        -- FOREIGN KEY not working
            _airbyte_data -> 'properties' ->> 'person_organization_id' as person_organization_id,        -- FOREIGN KEY not working
            _airbyte_data -> 'properties' ->> 'person_id_name' as person_id_name,        -- FOREIGN KEY not working
            _airbyte_data -> 'properties' ->> 'co_id' as co_id,
            (_airbyte_data -> 'closed')::boolean as referral_closed,
            date(
                nullif(_airbyte_data -> 'properties' ->> 'followupdate', '')
            ) as followup_date,
            _airbyte_data -> 'properties' ->> 'statustreatment' as status_treatment,
            _airbyte_data -> 'properties' ->> 'accessref' as accessref,
            _airbyte_data -> 'properties' ->> 'referralcategory' as referral_category,
            date(
                nullif(_airbyte_data -> 'properties' ->> 'referraldate', '')
            ) as referral_date,
            coalesce(
                nullif(_airbyte_data -> 'properties' ->> 'referral_placecat1', ''),
                nullif(_airbyte_data -> 'properties' ->> 'referral_placecat2', ''),
                nullif(_airbyte_data -> 'properties' ->> 'referral_placecat3', '')
            ) as referral_place,
            coalesce(
                nullif(_airbyte_data -> 'properties' ->> 'referralreason1', ''),
                nullif(_airbyte_data -> 'properties' ->> 'referralreason2', ''),
                nullif(_airbyte_data -> 'properties' ->> 'referralreason3', '')
            ) as referral_reason,
            _airbyte_data -> 'properties' ->> 'followup' as followup_required,
            case
                when
                    date(nullif(_airbyte_data -> 'properties' ->> 'followupdate', ''))
                    is not null
                then 'Yes'
                else 'No'
            end as referral_followed_up,
            _airbyte_data -> 'properties' ->> 'case_type' as case_type,
            _airbyte_ab_id,
            _airbyte_emitted_at
        from {{ source("commcare_common", "raw_case") }}
        where
            -- (_airbyte_data ->> 'id') = '5b84d949-0a34-46af-b385-b3b663c9ecc8'
            (_airbyte_data -> 'properties' ->> 'case_type') = 'sneharefollwuptemp'
            and (_airbyte_data -> 'properties' ->> 'referralcategory') = 'MWRA'
            /* removing test cases */
            and (_airbyte_data -> 'properties' ->> 'womanname') not like '%Demo%'
            and (_airbyte_data -> 'properties' ->> 'womanname') not like '%dummy%'
            and (_airbyte_data -> 'properties' ->> 'womanname') not like '%error%'
    )

-- select * from referral_cte
-- -- where person_organization_id is not null
-- order by _airbyte_emitted_at desc

{{ dbt_utils.deduplicate(
    relation='referral_cte',
    partition_by='referral_followup_id',
    order_by='_airbyte_emitted_at desc',
   )
}}