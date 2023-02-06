{{ config(
  materialized='table'
) }}

SELECT referral_followups.*,
c.clusterid,c.clustername,c.program_code,c.coid,c.womanname,c.aww_number,c.hh_number,c.anc_closed
FROM {{ref('anc_referral_followup_duplicates_removed')}} AS referral_followups 
LEFT JOIN {{ref('anc_case_duplicates_removed')}} AS c
ON referral_followups.womanid=c.womanid