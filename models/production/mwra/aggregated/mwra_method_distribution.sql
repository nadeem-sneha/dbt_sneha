with
    calendar as (select * from {{ ref("calendar_table") }}),
    cases as (select * from {{ ref("mwra_case") }}),

    open_case_list_for_each_month as (
        select *
        from calendar
        cross join cases
        where
            case_opened_date <= month_end_date
            and (case_closed_date is null or case_closed_date >= month_start_date)
    ),

    calculate_fp_method_distribution as (
        select
            month_start_date,
            program_code,
            clustername,
            coid,
            aww_number,
            last_known_fpmethod,
            count(case_id) as women_using_modern_methods
        from open_case_list_for_each_month
        where
            last_known_fpmethod in
                            (
                                'Copper-T',
                                'Female_sterilization',
                                'Injectable_Antara',
                                'Male_sterilization',
                                'nirodh',
                                'Other_Modern_methods_of_contraception',
                                'Pill'
                            )
        group by 1, 2, 3, 4, 5, 6
    )

select *
from calculate_fp_method_distribution

