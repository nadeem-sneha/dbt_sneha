with

    metrics as (select * from {{ ref('mwra_metrics_definition') }}),

    aggregate_to_cluster as (
        select
            month_start_date,
            month_end_date,
            program_code,
            clustername,
            sum(open_cases) as open_cases,
            sum(fp_eligible_women) as fp_eligible_women,
            sum(women_using_modern_methods) as women_using_modern_methods,
            sum(visited_cases) as visited_cases,
            sum(co_visits) as co_visits,
            sum(volunteer_visits) as volunteer_visits,
            sum(sent_for_referral) as sent_for_referral,
            sum(sent_for_referral_by_co) as sent_for_referral_by_co,
            sum(sent_for_referral_by_cag) as sent_for_referral_by_cag,
            sum(referrals_followed_up) as referrals_followed_up,
            sum(accessed_service) as accessed_service,
            sum(availed_service) as availed_service,
            sum(unvisited_open_cases) as unvisited_open_cases
        from
            metrics
        group by 1, 2, 3, 4
    ),

    aggregate_to_program_code as (
        select
            month_start_date,
            month_end_date,
            program_code,
            'All' as clustername,
            sum(open_cases) as open_cases,
            sum(fp_eligible_women) as fp_eligible_women,
            sum(women_using_modern_methods) as women_using_modern_methods,
            sum(visited_cases) as visited_cases,
            sum(co_visits) as co_visits,
            sum(volunteer_visits) as volunteer_visits,
            sum(sent_for_referral) as sent_for_referral,
            sum(sent_for_referral_by_co) as sent_for_referral_by_co,
            sum(sent_for_referral_by_cag) as sent_for_referral_by_cag,
            sum(referrals_followed_up) as referrals_followed_up,
            sum(accessed_service) as accessed_service,
            sum(availed_service) as availed_service,
            sum(unvisited_open_cases) as unvisited_open_cases
        from
            metrics
        group by 1, 2, 3, 4
    ),

    union_aggregated_metrics as (
        select * from aggregate_to_cluster
        union all
        select * from aggregate_to_program_code
    )

select
    * 
from union_aggregated_metrics