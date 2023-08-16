with

    metrics as (select * from {{ ref('mwra_metrics_clustered') }}),

    get_monthly_timespans as (
        select * from metrics
        where (month_end_date - month_start_date)::int < 31
    ),

    calculate_percentages as (
        select
            *,
            women_using_modern_methods * 1.0 / nullif(fp_eligible_women, 0) as pct_cpr,
            visited_cases * 1.0 / nullif(open_cases, 0) as pct_total_visits,
            volunteer_visits * 1.0 / nullif(open_cases, 0) as pct_cag_visits,
            co_visits * 1.0 / nullif(open_cases, 0) as pct_co_visits,
            sent_for_referral * 1.0 / nullif(open_cases, 0) as pct_referral,
            availed_service * 1.0 / nullif(referrals_followed_up, 0) as pct_availed_service,
            accessed_service * 1.0 / nullif(referrals_followed_up, 0) as pct_accessed_service,
            referrals_followed_up * 1.0 / nullif(sent_for_referral, 0) as pct_followed_up,
            sent_for_referral * 1.0 / nullif(open_cases, 0) as pct_reffered
        from get_monthly_timespans        
    )

select * from calculate_percentages