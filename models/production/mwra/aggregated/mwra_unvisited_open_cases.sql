with
    calendar as (select * from {{ ref("calendar_table") }}),
    cases as (select * from {{ ref("mwra_case") }}),
    visits as (select * from {{ ref("mwra_visit") }}),

    get_open_case_list as (
        select
            *
        from calendar
        cross join cases
        where
            case_opened_date <= month_end_date
            and (case_closed_date is null or (case_closed_date >= month_start_date))
    ),

    join_visits as (
        select
            get_open_case_list.*,
            visit_id,
            visit_date
        from get_open_case_list
        left join visits
            on visits.case_id = get_open_case_list.case_id
                and visit_date between month_start_date and month_end_date
    ),
    
    get_unvisited_open_cases as (
        select
            month_start_date,
            month_end_date,
            program_code,
            clustername,
            coid,
            aww_number,
            count(
                distinct
                case when visit_id is null then case_id
                else null
                end
            ) as unvisited_open_cases
        from
            join_visits
        group by 1, 2, 3, 4, 5, 6
    )

select *
from get_unvisited_open_cases

