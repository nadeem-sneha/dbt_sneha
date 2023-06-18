{{ config(
  materialized='table'
) }}

with base_referrals_cte as (SELECT referrals.*, c.clusterid,c.clustername,c.program_code,c.coid,c.aww_number,c.hh_number,c.anc_closed 
from {{ref('anc_referral_followup_normalized')}} as referrals
INNER JOIN {{ref('anc_case_normalized')}} as c
ON referrals.womanname=c.womanname),

additional_referrals_cte as (SELECT referrals.*, c.clusterid,c.clustername,c.program_code,c.coid,c.aww_number,c.hh_number,c.anc_closed 
from {{ref('anc_referral_followup_normalized')}} as referrals
LEFT JOIN {{ref('anc_case_normalized')}} as c
ON referrals.womanid=c.womanid WHERE referrals.id not in (SELECT id from base_referrals_cte))

select * from base_referrals_cte
UNION 
select * from additional_referrals_cte