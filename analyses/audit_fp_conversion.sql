with
    calendar as (select * from {{ ref("calendar_table") }}),
    cases as (select * from {{ ref("mwra_case") }}),
    visits as (select * from {{ ref("mwra_visit") }}),

    open_case_list_for_each_month as (
        select *
        from calendar
        cross join cases
        where
            case_opened_date <= month_end_date
            and (case_closed_date is null or case_closed_date >= month_start_date)
    ),

    join_open_cases_with_visits as (
        select
            open_case_list_for_each_month.*,
            visit_id,
            visit_date,
            fp_conversion,
            row_number() over (partition by case_id, month_start_date order by visit_date desc) as ov 
        from open_case_list_for_each_month
        left join visits using (case_id)
        where
            visit_date between month_start_date and month_end_date
            and visit_conducted_by = 'co'
            and fp_eligible = 'Eligible'
    )

select
    count(distinct case_id)
from join_open_cases_with_visits
where month_start_date = '2023-05-01'