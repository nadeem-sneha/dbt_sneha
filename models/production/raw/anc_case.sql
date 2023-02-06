{{ config(
  materialized='table'
) }}

WITH ordered_visits AS (
  SELECT visits.*, ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY visitdate DESC) AS ov
  FROM {{ref('anc_visit_duplicates_removed')}} AS visits 
)


SELECT  c.id,
        c.womanname,
        c.womanid,
        c.person_organizaton_id,
        c.age,
        c.clusterid,
        c.clustername,
        c.coid,
        c.program_code,
        c.program_name,
        c.hh_number,
        c.aww_number,
        c.closed,
        c.anc_enrolled,
        c.anc_reg,
        c.anc_closed,
        c.anc_closereason,
        c.high_risk_preg,
        last_visit.why_high_risk,
        last_visit.hb_grade,
        c.lmpdate,
        c.edddate,
        c.referral,
        c.referral_date, 
        c.referral_place, 
        c.referral_reason,
        c.referral_category,
        c.gravida_count,
        c.anc_closereason as pregoutcome,
        c.delivery_date,
        c.delivery_site,
        CASE 
              WHEN delivery_site = 'Home delivery' THEN 'Home'
              WHEN delivery_site IS NOT NULL THEN 'Institutional'
        END AS delivery_site_type,
        c.case_type,
        c.individual_category,
        c.service_registration,
        c.case_opened_date,
        c.anc_identify_date,
        date_part('months',age(current_date, c.lmpdate)) AS pregnantmonth,
        CASE 
              when (date_part('months',age(current_date, c.lmpdate))>=0 AND date_part('months',age(current_date, c.lmpdate))<=3 AND c.anc_closed IS NULL )then 'First trimester'
              when (date_part('months',age(current_date, c.lmpdate))>=4 AND date_part('months',age(current_date, c.lmpdate))<=6 AND c.anc_closed IS NULL )then 'Second trimester'
              when (date_part('months',age(current_date, c.lmpdate))>=7 AND date_part('months',age(current_date, c.lmpdate))<=10 AND c.anc_closed IS NULL) then 'Third trimester'
              when (date_part('months',age(current_date, c.lmpdate))>10 AND c.anc_closed IS NULL) then 'Over-due'
              ELSE 'NA'
        END AS trimester,
        last_visit.lastvisitdate AS last_anc_visit_date, 
        current_date-last_visit.lastvisitdate AS days_from_last_visit,
        last_visit.lastvisitreason,
        last_visit.last_visit_conducted_by,
        CASE 
              when (extract(month FROM last_visit.lastvisitdate) = extract(month from current_date))
               AND (extract(year FROM last_visit.lastvisitdate) = extract(year from current_date)) 
               then 'Visited' 
               ELSE 'Not Yet Visited' 
        END AS currentmonthvisitStatus


FROM {{ref('anc_case_duplicates_removed')}} AS c
LEFT JOIN
(select caseid,visitdate AS lastvisitdate,conducted_by AS last_visit_conducted_by, visitreason AS lastvisitreason,why_high_risk,hb_grade from ordered_visits where ov=1 ) AS last_visit 
ON last_visit.caseid = c.id
