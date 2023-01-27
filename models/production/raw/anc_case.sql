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
        form.why_high_risk,
        /*c.why_high_risk,*/
        c.woman_bmi_grade,
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
              WHEN lower(delivery_site) like '%home%' THEN 'Home'
              WHEN delivery_site IS NOT NULL THEN 'Institutional'
        END AS delivery_site_type,
        c.case_type,
        date_part('months',age(current_date, c.lmpdate)) AS pregnantmonth,
        CASE 
              when (date_part('months',age(current_date, c.lmpdate))>=0 AND date_part('months',age(current_date, c.lmpdate))<=3 AND c.anc_closed IS NULL )then 'First trimester'
              when (date_part('months',age(current_date, c.lmpdate))>=4 AND date_part('months',age(current_date, c.lmpdate))<=6 AND c.anc_closed IS NULL )then 'Second trimester'
              when (date_part('months',age(current_date, c.lmpdate))>=7 AND date_part('months',age(current_date, c.lmpdate))<=10 AND c.anc_closed IS NULL) then 'Third trimester'
              when (date_part('months',age(current_date, c.lmpdate))>10 AND c.anc_closed IS NULL) then 'Over-due'
              ELSE 'NA'
        END AS trimester,
        form.lastvisitdate AS last_anc_visit_date, 
        current_date-form.lastvisitdate AS days_from_last_visit,
        form.lastvisitreason,
        form.last_visit_conducted_by,
        CASE 
              when (extract(month FROM form.lastvisitdate) = extract(month from current_date))
               AND (extract(year FROM form.lastvisitdate) = extract(year from current_date)) 
               then 'Visited' 
               ELSE 'Not Yet Visited' 
        END AS currentmonthvisitStatus


FROM {{ref('case_duplicates_removed')}} AS c
LEFT JOIN
(select caseid,visitdate AS lastvisitdate,conducted_by AS last_visit_conducted_by, visitreason AS lastvisitreason,why_high_risk from ordered_visits where ov=1 ) AS form 
ON form.caseid = c.id
