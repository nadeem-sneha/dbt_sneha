{{ config(
  materialized='view'
) }}

select *,'All' as clustername
from {{ metrics.calculate(
    [   metric('pct_total_visits'),
        metric('pct_co_visits'),
        metric('pct_volunteer_visits'),
        metric('pct_volunteer_referral_visits'),
        metric('pct_co_referral_visits'),
        metric('pct_volunteers_involved_in_anc_referral_count'),
        metric('pct_thr_referral'),
        metric('pct_thr_visits'),
        metric('pct_institutional_delivery'),
        metric('pct_anemia_tested'),
        metric('pct_early_registration')
    ],
    grain='month',
    dimensions=['program_code']
) }}




