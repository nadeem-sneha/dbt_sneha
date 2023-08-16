with
    source as (select * from {{ ref('mwra_metrics_definition') }}),

    calculate_totals as (
        select
            sum(open_cases) as open_cases,
            sum(visited_cases) as visited_cases,
            sum(co_visits) as co_visits,
            sum(volunteer_visits) as volunteer_visits,
            sum(sent_for_referral) as sent_for_referral,
            sum(sent_for_referral_by_co) as sent_for_referral_by_co,
            sum(sent_for_referral_by_cag) as sent_for_referral_by_cag,
            sum(fp_eligible_women) as fp_eligible_women,
            sum(referrals_followed_up) as referrals_followed_up,
            sum(accessed_service) as accessed_service,
            sum(availed_service) as availed_service,
            sum(women_using_modern_methods) as women_using_modern_methods,
            sum(women_using_modern_methods) / sum(fp_eligible_women) * 100 as CPR
        from source
        where
            month_start_date = '2023-05-01'
            and month_end_date = '2023-05-31'
    )

select *
from calculate_totals

