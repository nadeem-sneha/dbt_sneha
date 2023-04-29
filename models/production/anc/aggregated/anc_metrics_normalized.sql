{{ config(
  materialized='table'
) }}

select date_month,program_code,clustername,
unnest(array[
    '1. ANC Women Visited',
    '2. ANC women visted by COs',
    '3. ANC Women Visited by Volunteers',
    '4. ANC Volunteer Visits with Referrals',
    '5. ANC CO Visits with Referrals',
    '6. Volunteers involved in Referrals',
    '7. Referrals for THR',
    '8. isits with ANC women reporting THR usage',
    '9. Institutional Delivery',
    '10.Anemia Tested',
    '11. Early registration'
    ]) AS indicator,
unnest(array[
    pct_total_visits,
    pct_co_visits,
    pct_volunteer_visits,
    pct_volunteer_referral_visits,
    pct_co_referral_visits,
    pct_volunteers_involved_in_anc_referral_count,
    pct_thr_referral,
    pct_thr_visits,
    pct_institutional_delivery,
    pct_anemia_tested,
    pct_early_registration
    ]) AS score
FROM {{ref('anc_metrics')}}
where date_month IS NOT NULL



