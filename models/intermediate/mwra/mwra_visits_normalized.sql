{{
    config(
        indexes=[{"columns": ["_airbyte_ab_id"], "type": "hash"}],
    )
}}

with
    source as (select * from {{ source("commcare_mwra", "raw_mwra_visit") }}),

    extract_from_data as (
        select
            _airbyte_ab_id,
            _airbyte_emitted_at,
            (_airbyte_data ->> 'id') as visit_id,   -- PRIMARY KEY
            (_airbyte_data -> 'form' -> 'case_autoload_case1' -> 'case' ->> '@case_id') as case_id,
            (_airbyte_data -> 'form' ->> 'womanid') as woman_id,
            (_airbyte_data -> 'form' ->> 'womanname') as woman_name,
            (_airbyte_data -> 'form' ->> 'cag_id') as cag_id,
            (_airbyte_data -> 'form' ->> 'hvconduct') as visit_conducted_by,
            (_airbyte_data -> 'form' ->> 'load_woman_name_id') as load_woman_name_id,
            date(nullif(_airbyte_data -> 'form' ->> 'visitdate', '')) as visit_date,
            date(
                nullif(_airbyte_data -> 'form' ->> 'load_task_next_visit_date', '')
            ) as next_visit_date,
            (_airbyte_data -> 'form' ->> 'fp') as fp,
            (_airbyte_data -> 'form' ->> 'fpmethod') as fpmethod,
            (_airbyte_data -> 'form' ->> 'fpmethodnot') as fpmethodnot,
            (_airbyte_data -> 'form' ->> 'current_fpstatus') as current_fpstatus,
            (_airbyte_data -> 'form' ->> 'MSeligible') as mseligible,
            (_airbyte_data -> 'form' ->> 'userstatus') as user_status,
            (_airbyte_data -> 'form' ->> 'fpvisitreason') as fpvisitreason,
            (_airbyte_data -> 'form' ->> 'referral') as referral,
            coalesce(
                nullif(_airbyte_data -> 'form' ->> 'referral_reasoncat1', ''),
                nullif(_airbyte_data -> 'form' ->> 'referral_reasoncat2', ''),
                nullif(_airbyte_data -> 'form' ->> 'referral_reasoncat3', '')
            ) as referral_reason
        from source
        where
            true
            and (_airbyte_data ->> 'archived')::boolean = false
            /* removing test cases */
            and (_airbyte_data -> 'form' ->> 'womanname') not like '%Demo%'
            and (_airbyte_data -> 'form' ->> 'womanname') not like '%dummy%'
            and (_airbyte_data -> 'form' ->> 'womanname') not like '%error%'
            and (_airbyte_data -> 'form' -> 'case_autoload_case1' -> 'case' ->> '@case_id') not in (
                    select caseid from {{ ref("incorrectly_screened_case_normalized") }}
                )
        order by _airbyte_emitted_at desc
    ),
        
    add_fp_eligible as (
        select 
            *,
            case
                when 
                current_fpstatus in (
                        'Any type illness',
                        'Divorce',
                        'Dont_know',
                        'Husband away',
                        'Infrequent sex',
                        'Missconception',
                        'Opposition',
                        'Other',
                        'Recent delivery',
                        'Religious',
                        'Side effect',
                        'traditional_method',
                        'Widow',
                        'Antara',
                        'Condom',
                        'CoperT',
                        'Female st',
                        'Male st',
                        'Other modern',
                        'Goli'
                    )
                then 'Eligible'
                else 'Not Eligible'
            end as fp_eligible
        from extract_from_data
    )

{{ dbt_utils.deduplicate(
    relation='add_fp_eligible',
    partition_by='visit_id',
    order_by='_airbyte_emitted_at desc',
   )
}}