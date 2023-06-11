{{
    config(
        materialized="table",
        schema="dev_goalkeep",
    )
}}

with
    mwra_case as (select * from {{ ref("mwra_case_normalized") }}),
    
    mwra_referral_followup as (
        select * from {{ ref("mwra_referral_followup_normalized") }}
    ),

    get_case_details as (
        select
            mwra_referral_followup.*,
            -- mwra_referral_followup.referral_followup_id,
            -- mwra_referral_followup.case_name,
            -- mwra_referral_followup.woman_id,
            -- mwra_referral_followup.case_type,
            -- mwra_referral_followup.referral_closed,
            -- mwra_referral_followup.referral_category,
            -- mwra_referral_followup.referral_reason,
            -- mwra_referral_followup.referral_date,
            -- mwra_referral_followup.referral_place,
            -- mwra_referral_followup.status_treatment,
            -- mwra_referral_followup.followup_required,
            -- mwra_referral_followup.referral_followed_up,
            -- mwra_referral_followup.accessref,
            mwra_case.womanname,
            mwra_case.hh_number,
            mwra_case.aww_number,
            mwra_case.clusterid,
            mwra_case.coid
        from mwra_referral_followup
            left join mwra_case using (case_name)
    ),
    add_status_fields as (
        select
            *,
            case
                when
                    status_treatment
                    in ('Service_availed', 'Currently_availing')
                then 'Service availed'
                else 'Service not availed'
            end as availed_status,
            case
                when
                    accessref in (
                        'Yes_referred_place', 'Yes_another_hospital', 'Hospital_Refusal'
                    )
                then 'Service accessed'
                else 'Service not accessed'
            end as accessed_status
        from get_case_details
    )


select *
from add_status_fields
