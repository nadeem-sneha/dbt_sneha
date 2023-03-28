{{ config(
  materialized='table'
) }}

/*
select * 
from {{ metrics.calculate(
    metric('open_case_count'),
    grain='month',
    dimensions=['clustername']
) }}
*/

select * 
from {{ metrics.calculate(
    [   metric('open_case_count'),
        metric('total_visits_count'),
        metric('co_visits_count'),
        metric('volunteer_visits_count'),
        metric('anemia_tested_count'),
        metric('volunteer_open_count'),
        metric('volunteers_involved_in_anc_referral_count'),
        metric('volunteer_referral_count'),
        metric('co_referral_count'),
        metric('thr_referral_count'),
        metric('thr_count'),
        metric('delivery_count'),
        metric('institutional_delivery_count'),
        metric('anemia_tested_count'),
        metric('early_registration_count')
    ],
    grain='month',
    dimensions=[]
) }}

/*'program_code','clustername','coid','aww_number' */
