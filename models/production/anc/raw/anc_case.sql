{{ config(
  materialized='table'
) }}

-- use rank to get the last visit
WITH ordered_visits AS (
  SELECT visits.*, ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY visitdate DESC) AS ov
  FROM {{ref('anc_visit_normalized')}} AS visits 
  WHERE visits.visitreason='ANC'
),
-- get last visit with non null hbgrade
ordered_visits_hb_grade_last AS (
  SELECT visits.*, ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY visitdate DESC) AS ov
  FROM {{ref('anc_visit_normalized')}} AS visits 
  WHERE visits.hb_grade IS NOT NULL AND visits.visitreason='ANC'
),
-- get earliest visit with non null hbgrade
ordered_visits_hb_grade_earliest AS (
  SELECT visits.*, ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY visitdate ASC) AS ov
  FROM {{ref('anc_visit_normalized')}} AS visits 
  WHERE visits.hb_grade IS NOT NULL AND visits.visitreason='ANC'
),
-- get last visit with non null anc_reg_trimester
ordered_visits_anc_reg_trimester AS (
  SELECT visits.*, ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY visitdate DESC) AS ov
  FROM {{ref('anc_visit_normalized')}} AS visits 
  WHERE visits.anc_reg_trimester IS NOT NULL AND visits.visitreason='ANC'
),
-- get last visit with non null why high risk reason
ordered_visits_why_high_risk AS (
  SELECT visits.*, ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY visitdate DESC) AS ov
  FROM {{ref('anc_visit_normalized')}} AS visits 
  WHERE (visits.why_high_risk IS NOT NULL) AND (visits.visitreason='ANC')
),
-- get visit with last anc close as visit reason to find visit date and use as anc close date
ordered_visits_anc_close AS (
  SELECT visits.*, ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY visitdate DESC) AS ov
  FROM {{ref('anc_visit_normalized')}} AS visits
  WHERE visits.visitreason='Close_case'
)

SELECT  c.id,
        c.case_name,
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
        -- pull identify date frpom registration data
        r.anc_identify_date,
        -- pull anc registration data from visit
        last_non_null_anc_reg_trimester_visit.anc_reg,
        last_non_null_anc_reg_trimester_visit.anc_reg_trimester,
        c.anc_closed,
        c.anc_closereason,
        c.high_risk_preg,
        last_non_null_why_high_risk_visit.why_high_risk,
        last_non_null_hb_grade_visit.hb_grade,
        last_non_null_hb_grade_visit.hb_grade_date,
        earliest_non_null_hb_grade_visit.earliest_hb_grade,
        earliest_non_null_hb_grade_visit.earliest_hb_grade_date,
        c.lmpdate,
        c.edddate,
        c.gravida_count,
        last_anc_close_visit.visitdate AS anc_close_date,
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
        c.date_closed,        
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

FROM {{ref('anc_case_normalized')}} AS c
LEFT JOIN
(select caseid, visitdate AS lastvisitdate,conducted_by AS last_visit_conducted_by, visitreason AS lastvisitreason,why_high_risk,hb_grade from ordered_visits where ov=1 ) AS last_visit 
ON last_visit.caseid = c.id AND last_visit.lastvisitdate > c.lmpdate
LEFT JOIN
(select caseid, anc_reg, anc_reg_trimester,visitdate from ordered_visits_anc_reg_trimester where ov=1) as last_non_null_anc_reg_trimester_visit
ON last_non_null_anc_reg_trimester_visit.caseid=c.id AND last_non_null_anc_reg_trimester_visit.visitdate > c.lmpdate
LEFT JOIN
(select caseid, hb_grade,visitdate AS hb_grade_date from ordered_visits_hb_grade_last where ov=1) as last_non_null_hb_grade_visit
ON last_non_null_hb_grade_visit.caseid=c.id AND last_non_null_hb_grade_visit.hb_grade_date > c.lmpdate
LEFT JOIN 
(select caseid, hb_grade AS earliest_hb_grade,visitdate AS earliest_hb_grade_date from ordered_visits_hb_grade_earliest where ov=1) as earliest_non_null_hb_grade_visit
ON earliest_non_null_hb_grade_visit.caseid=c.id AND earliest_non_null_hb_grade_visit.earliest_hb_grade_date > c.lmpdate
LEFT JOIN
(select caseid, why_high_risk,visitdate from ordered_visits_why_high_risk where ov=1) as last_non_null_why_high_risk_visit
ON last_non_null_why_high_risk_visit.caseid=c.id AND last_non_null_why_high_risk_visit.visitdate > c.lmpdate
LEFT JOIN
(select caseid,visitdate from ordered_visits_anc_close where ov=1) as last_anc_close_visit
ON last_anc_close_visit.caseid=c.id AND last_anc_close_visit.visitdate > c.lmpdate
LEFT JOIN 
{{ref('anc_registration_normalized')}}r
ON r.caseid=c.id
