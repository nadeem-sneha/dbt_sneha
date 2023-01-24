{{ config(
  materialized='table'
) }}

SELECT  c.id,
        c.womanname,
        c.womanid,
        c.age,
        c.clusterid,
        c.clustername,
        c.coid,
        c.closed,
        c.anc_closed,
        c.anc_closereason,
        c.high_risk_preg,
        c.woman_bmi_grade,
        c.why_high_risk,
        c.lmpdate,
        c.referral,
        c.referral_date, 
        c.referral_place, 
        c.referral_reason,
        c.referral_category,
        c.gravida_count,
        c.pregoutcome,
        c.delivery_date,
        c.delivery_site,
        CASE 
              WHEN lower(delivery_site) like '%home%' THEN 'Home'
              ELSE 'Institutional'
        END AS delivery_site_type,
        c.case_type,
        date_part('months',age(current_date, c.lmpdate)) AS pregnantmonth,
        CASE 
              when (date_part('months',age(current_date, c.lmpdate))>=0 AND date_part('months',age(current_date, c.lmpdate))<=3 AND c.closed = false )then 'First trimester'
              when (date_part('months',age(current_date, c.lmpdate))>=4 AND date_part('months',age(current_date, c.lmpdate))<=6 AND c.closed = false)then 'Second trimester'
              when (date_part('months',age(current_date, c.lmpdate))>=7 AND date_part('months',age(current_date, c.lmpdate))<=10 AND c.closed = false) then 'Third trimester'
        ELSE 'NA' END AS trimester,
        form.lastvisitdate AS last_anc_visit_date, 
        current_date-form.lastvisitdate AS days_from_last_visit,
        CASE 
              when (extract(month FROM form.lastvisitdate) = extract(month from current_date))
               AND (extract(year FROM form.lastvisitdate) = extract(year from current_date)) 
               then 'Visited' 
               ELSE 'Not Yet Visited' END AS currentmonthvisitStatus
FROM {{ref('case_duplicates_removed')}} AS c
LEFT JOIN
(select caseid,max(visitdate) as lastvisitdate from {{ref('anc_visit_duplicates_removed')}} where visitreason ='ANC' group by caseid ) AS form 
ON form.caseid = c.id
