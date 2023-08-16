with
    calendar as (select * from {{ ref("calendar_table") }}),
    cases as (select * from {{ ref("mwra_case") }}),
    visits as (select * from {{ ref("mwra_visit") }}),
    referral_followups as (select * from {{ ref('mwra_referral_followup') }}),

    unvisited_open_cases as (select * from {{ ref('mwra_unvisited_open_cases') }}),

    open_case_list_for_each_month as (
        select *
        from calendar
        cross join cases
        where
            case_opened_date <= month_end_date
            and (case_closed_date is null or case_closed_date >= month_start_date)
    ),

    case_metrics_for_open_cases as (
        select 
        month_start_date,
        month_end_date,
        program_code,
        clustername,
        coid,
        aww_number,
        count(case_id) as open_cases,
        count(
            distinct
            case
                when last_known_fp_eligible = 'Eligible'
                then case_id
                else null
            end
        ) as fp_eligible_women,
        count(
            distinct
            case
                when last_known_fpmethod in
                (
                    'Copper-T',
                    'Female_sterilization',
                    'Injectable_Antara',
                    'Male_sterilization',
                    'nirodh',
                    'Other_Modern_methods_of_contraception',
                    'Pill'
                )
                then case_id
                else null
            end
        ) as women_using_modern_methods
        from open_case_list_for_each_month
        group by 1, 2, 3, 4, 5, 6
    ),

    join_open_cases_with_visits as (
        select
            open_case_list_for_each_month.*,
            visit_id,
            visit_date,
            visit_conducted_by,
            fp,
            fpmethod,
            fpmethodnot,
            current_fpstatus,
            mseligible,
            -- fp_eligible,
            user_status,
            fpvisitreason,
            referral,
            referral_reason,
            prev_fp,
            prev_fpmethod,
            prev_mseligible,
            n_minus_2_fpmethod,
            -- fp_conversion,
            days_since_last_co_visit
        from open_case_list_for_each_month
        left join visits using (case_id)
        where visit_date between month_start_date and month_end_date
    ),

    visit_metrics as (
        select
            month_start_date,
            month_end_date,
            program_code,
            clustername,
            coid,
            aww_number,
            count(distinct case_id) as visited_cases,
            count(
                distinct
                case
                    when visit_conducted_by = 'co'
                    then case_id
                    else null
                end
            ) as co_visits,
            count(
                distinct
                case
                    when visit_conducted_by = 'volunteer'
                    then case_id
                    else null
                end
            ) as volunteer_visits,
            count(
                distinct
                case
                    when referral = 'Yes' and referral_reason = 'FP_Method'
                    then case_id
                    else null
                end
            ) as sent_for_referral,
            count(
                distinct
                case
                    when referral = 'Yes' and referral_reason = 'FP_Method' and visit_conducted_by = 'co'
                    then case_id
                    else null
                end
            ) as sent_for_referral_by_co,
            count(
                distinct
                case
                    when referral = 'Yes' and referral_reason = 'FP_Method' and visit_conducted_by = 'volunteer'
                    then case_id
                    else null
                end
            ) as sent_for_referral_by_cag
        from join_open_cases_with_visits
        where visit_date between month_start_date and month_end_date
        group by 1, 2, 3, 4, 5, 6
    ),

    cross_join_referral_followups as (select * from calendar cross join referral_followups),

    referral_followup_metrics as (
        select
            month_start_date,
            month_end_date,
            program_code,
            clustername,
            coid,
            aww_number,
            count(
                distinct
                case
                    when referral_followed_up = 'Yes' and referral_reason = 'FP_Method'
                    then case_name
                    else null
                end
            ) as referrals_followed_up,
            count(
                distinct
                case
                    when accessed_status = 'Yes' and referral_reason = 'FP_Method'
                    then case_name
                    else null
                end
            )as accessed_service,
            count(
                distinct
                case
                    when availed_status = 'Yes' and referral_reason = 'FP_Method'
                    then case_name
                    else null
                end
            )as availed_service
        from
            cross_join_referral_followups
        where
            referral_date between month_start_date and month_end_date
            and followup_date between month_start_date and month_end_date
        group by 1, 2, 3, 4, 5, 6
    ),

    join_metrics as (
        select *
        from case_metrics_for_open_cases
        left outer join
            visit_metrics using (
                month_start_date,
                month_end_date,
                program_code,
                clustername,
                coid,
                aww_number
            )
        left outer join
            referral_followup_metrics using (
                month_start_date,
                month_end_date,
                program_code,
                clustername,
                coid,
                aww_number
            )
        left outer join unvisited_open_cases using (
                month_start_date,
                month_end_date,
                program_code,
                clustername,
                coid,
                aww_number
            )
    )

select *
from join_metrics

