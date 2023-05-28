{{ config(
  materialized='view'
) }}

select month_start_date,month_end_date,program_code,'All' as clustername,
SUM(open_case_count) as open_case_count,
SUM(total_visits_count) as total_visits_count,
SUM(co_visits_count) as co_visits_count,
SUM(volunteer_visits_count) as volunteer_visits_count,
SUM(volunteer_open_count) as  volunteer_open_count,
SUM(volunteers_involved_in_anc_referral_count) as volunteers_involved_in_anc_referral_count,
SUM(volunteer_referral_count) as volunteer_referral_count,
SUM(co_referral_count) as co_referral_count,
SUM(thr_referral_count) as thr_referral_count,
SUM(thr_count) as thr_count,
SUM(delivery_count) as delivery_count,
SUM(institutional_delivery_count) as institutional_delivery_count,
SUM(anemia_tested_count) as anemia_tested_count,
SUM(early_registration_count) as early_registration_count,
SUM(total_visits_count)::float/SUM(open_case_count) AS pct_total_visits,
SUM(co_visits_count)::float/SUM(open_case_count) AS pct_co_visits,
SUM(volunteer_visits_count)::float/SUM(open_case_count) AS pct_volunteer_visits,
SUM(volunteer_referral_count)::float/SUM(open_case_count) AS pct_volunteer_referral_visits,
SUM(co_referral_count)::float/SUM(open_case_count) AS pct_co_referral_visits,
SUM(volunteers_involved_in_anc_referral_count)::float/SUM(volunteer_open_count) AS pct_volunteers_involved_in_anc_referral_count,
SUM(thr_referral_count)::float/SUM(open_case_count) AS pct_thr_referral,
SUM(thr_count)::float/SUM(open_case_count) AS pct_thr_visits,
SUM(institutional_delivery_count)::float/SUM(delivery_count) AS pct_institutional_delivery,
SUM(anemia_tested_count)::float/SUM(open_case_count) AS pct_anemia_tested,
SUM(early_registration_count)::float/SUM(open_case_count) AS pct_early_registration
FROM {{ ref('anc_metrics_definition') }}
GROUP BY month_start_date,month_end_date, program_code
ORDER BY program_code, month_start_date,month_end_date, clustername
