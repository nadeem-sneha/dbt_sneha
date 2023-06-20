{{
    config(
        materialized="table"
    )
}}

with
    calendar as (select * from {{ ref("calendar_table") }}),
    cases as (select * from {{ ref("mwra_case") }}),
    visits as (select * from {{ ref("mwra_visit") }}),

    cross_join_cases as (select * from calendar cross join cases),

    case_dependent_metrics as (
        select
            month_start_date,
            month_end_date,
            program_code,
            clustername,
            coid,
            -- last_visit_conducted_by,
            aww_number,
            count(case_id) as open_cases,
            count(
                case
                    when last_known_mseligible = 'Eligible'
                    then case_id
                    else null
                end
            ) as fp_eligible_women,
            count(
                case
                    when last_known_mseligible = 'Eligible' and last_known_fpmethod is not null
                    then case_id
                    else null
                end
            ) as women_using_modern_methods
        from cross_join_cases
        where
            case_opened_date <= month_end_date
            and (case_closed_date is null or (case_closed_date >= month_start_date))
        -- and case_id = '0008eb74-6382-44a7-adef-e95928e4e258'
        group by 1, 2, 3, 4, 5, 6
    ),

    cross_join_visits as (select * from calendar cross join visits),

    visit_dependent_metrics as (
        select
            month_start_date,
            -- month_end_date,
            program_code,
            clustername,
            coid,
            -- visit_conducted_by,
            aww_number,
            count(distinct case_id) as visited_cases
        from cross_join_visits
        where visit_date between month_start_date and month_end_date
        group by 1, 2, 3, 4, 5
    ),

    join_metrics as (
        select *
        from case_dependent_metrics
        left outer join
            visit_dependent_metrics using (
                month_start_date,
                -- month_end_date
                program_code,
                clustername,
                coid,
                -- visit_conducted_by
                aww_number
            )
    )

select *
from join_metrics
