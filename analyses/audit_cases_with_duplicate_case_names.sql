select
    referral_followup_id as unique_field,
    case_name,
    count(*) as n_records

from "sneha_common"."dev_goalkeep"."mwra_referral_followup"
where referral_followup_id is not null
group by referral_followup_id, 2
having count(*) > 1