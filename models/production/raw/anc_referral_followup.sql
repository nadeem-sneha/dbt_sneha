{{ config(
  materialized='table'
) }}

SELECT referrals.*, c.clusterid,c.clustername,c.program_code,c.coid,c.womanname,c.aww_number,c.hh_number,c.anc_closed 
from {{ref('anc_referral_followup_duplicates_removed')}} as referrals
LEFT JOIN {{ref('anc_case_duplicates_removed')}} as c
ON referrals.womanid=c.womanid
