with
    create_date_series as (
        {{
            dbt_utils.date_spine(
                datepart="month",
                start_date="cast('2022-01-01' as date)",
                end_date="CURRENT_DATE + INTERVAL '1 MONTH'",
            )
        }}
    ),

    get_month_end_date as (
        select
            (
                date_month + interval '1 month' - interval '1 day'
            )::date as month_end_date
        from create_date_series
    ),

    get_all_possible_monthly_date_ranges as (
        select
            date_month::date as month_start_date,
            month_end_date
        from create_date_series
        cross join get_month_end_date
        where date_month < month_end_date
    )

select *
from get_all_possible_monthly_date_ranges
