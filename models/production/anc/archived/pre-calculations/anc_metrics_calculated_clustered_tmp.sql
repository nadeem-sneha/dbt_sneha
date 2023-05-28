{{ config(
  materialized='view'
) }}

select * 
from {{ metrics.calculate(
    [   
        metric('open_case_count'),
        metric('total_visits_count'),
        metric('co_visits_count'),
        metric('volunteer_visits_count'),
        metric('volunteer_open_count'),
        metric('volunteers_involved_in_anc_referral_count'),
        metric('volunteer_referral_count'),
        metric('co_referral_count'),
        metric('thr_referral_count'),
        metric('thr_count'),
        metric('delivery_count'),
        metric('institutional_delivery_count'),
        metric('anemia_tested_count'),
        metric('early_registration_count'),
        metric('pct_total_visits'),
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
    dimensions=['program_code','clustername']
)}}
