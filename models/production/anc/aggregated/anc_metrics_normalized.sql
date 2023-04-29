{{ config(
  materialized='table'
) }}

select date_month,program_code,clustername,
unnest(array[
    'ANC Women Visited',
    'ANC women visted by COs',
    'ANC Women Visited by Volunteers',
    'ANC Volunteer Visits with Referrals',
    'ANC CO Visits with Referrals',
    'Volunteers involved in Referrals'
    'Referrals for THR',
    'Visits with ANC women reporting THR usage',
    'Institutional Delivery',
    'Anemia Tested',
    'Early registration'
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
    pct_early_registration]) AS score
FROM {{ref('anc_metrics')}}



