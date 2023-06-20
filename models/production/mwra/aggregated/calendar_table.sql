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

    add_month_end_date as (
        select
            date_month::date as month_start_date,
            (
                date_month + interval '1 month' - interval '1 day'
            )::date as month_end_date
        from create_date_series
    )

select *
from add_month_end_date
