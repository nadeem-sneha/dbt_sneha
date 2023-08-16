with
    metrics as (select * from {{ ref('mwra_metrics_clustered') }}),

    normalize as (
        select
            month_start_date,
            month_end_date,
            program_code,
            clustername,
            unnest(
                array[
                    '1. Open Cases',
                    '2. Visited Cases',
                    '3. CO Visits',
                    '4. Volunteer Visits',
                    '5. Sent For Referral',
                    '5a. Sent For Referral by CAG',
                    '5b. Sent For Referral By CO',
                    '6. Referrals Followed Up',
                    '7. Accessed Service',
                    '8. Availed Service',
                    '9. FP Eligible Women',
                    '10. Women Using Modern Methods',
                    '11. Unvisited Open Cases'
                ]
            ) as indicator,
            unnest(
                array[
                    open_cases,
                    visited_cases,
                    co_visits,
                    volunteer_visits,
                    sent_for_referral,
                    sent_for_referral_by_cag,
                    sent_for_referral_by_co,
                    referrals_followed_up,
                    accessed_service,
                    availed_service,
                    fp_eligible_women,
                    women_using_modern_methods,
                    unvisited_open_cases
                ]
            ) as count,
            unnest(
                array[
                    open_cases,             -- denominator for open_cases
                    open_cases,             -- denominator for visited_cases
                    open_cases,             -- denominator for co_visits,
                    open_cases,             -- denominator for volunteer_visits,
                    open_cases,             -- denominator for sent_for_referral,
                    sent_for_referral,      -- denominator for sent_for_referral_by_co,
                    sent_for_referral,      -- denominator for sent_for_referral_by_cag,
                    sent_for_referral,      -- denominator for referrals_followed_up,
                    referrals_followed_up,  -- denominator for accessed_service,
                    referrals_followed_up,  -- denominator for availed_service,
                    open_cases,             -- denominator for fp_eligible_women,
                    fp_eligible_women,      -- denominator for women_using_modern_methods,
                    open_cases              -- denominator for unvisited_open_cases
                ]
            ) as total
        from metrics
    ),

    calculate_percentage as (
        select
            *,
            count / nullif(total, 0) as score
        from normalize
    )

select *
from calculate_percentage
