with
    mwra_visits as (select * from {{ ref("mwra_visits_normalized") }}),
    mwra_cases as (select * from {{ ref("mwra_case_normalized") }}),

    get_case_details as (
        select
            mwra_visits.*,
            mwra_cases.program_code,
            mwra_cases.person_name,
            mwra_cases.hh_number,
            mwra_cases.aww_number,
            mwra_cases.clustername,
            mwra_cases.clusterid,
            mwra_cases.coid
        from mwra_visits
            left join mwra_cases using (case_id)
    ),
    filter_visits_by_co as (
        select *
        from get_case_details
        where visit_conducted_by = 'co'
    ),
    get_previous_visit_details as (
        select
            visit_id,
            fpmethod,
            lag(fp) over (
                partition by case_id order by visit_date
            ) as prev_fp,
            lag(fpmethod) over (
                partition by case_id order by visit_date
            ) as prev_fpmethod,
            lag(mseligible) over (
                partition by case_id order by visit_date
            ) as prev_mseligible,
            max(visit_date) over (
                partition by case_id
            ) as last_mwra_visit_date,
            lag(fpmethod, 2) over (
                partition by case_id order by visit_date
            ) as n_minus_2_fpmethod
        from filter_visits_by_co
    ),
    get_fp_conversion as (
        select
            visit_id,
            prev_fp,
            prev_fpmethod,
            prev_mseligible,
            last_mwra_visit_date,
            n_minus_2_fpmethod,
            case
                when prev_fpmethod is null and fpmethod is not null then 'new user'
                when prev_fpmethod is not null and fpmethod is null then 'relapse non user'
                when
                    prev_fpmethod != fpmethod
                    and prev_fpmethod is not null
                    and fpmethod is not null
                then 'method change'
                when
                    prev_fpmethod = fpmethod
                    and prev_fpmethod is not null
                    and fpmethod is not null
                then 'method same'
                else 'non user'
            end as fp_conversion
        from get_previous_visit_details
    ),
    get_days_since_last_co_visit as (
        select
            *,
            date_part(
                'day', age(now()::date,last_mwra_visit_date)
            ) as days_since_last_co_visit
        from get_fp_conversion
    ),
    join_co_visit_dimensions_to_all_visits as (
        select
            *
        from get_case_details
            left join get_days_since_last_co_visit using (visit_id)
    )

select 
*
from join_co_visit_dimensions_to_all_visits