with
    mwra_case as (select * from {{ ref("mwra_case_normalized") }}),
    mwra_referral_followup as (
        select * from {{ ref("mwra_referral_followup_normalized") }}
    ),

    get_case_details_using_case_name as (
        select
            mwra_referral_followup.*,
            mwra_case.person_name,
            mwra_case.hh_number,
            mwra_case.aww_number,
            mwra_case.program_code,
            mwra_case.clustername,
            mwra_case.clusterid,
            mwra_case.coid
        from mwra_referral_followup
            inner join mwra_case using (case_name)
    ),

    get_case_details_using_woman_id as (
        select
            mwra_referral_followup.*,
            mwra_case.person_name,
            mwra_case.hh_number,
            mwra_case.aww_number,
            mwra_case.program_code,
            mwra_case.clustername,
            mwra_case.clusterid,
            mwra_case.coid
        from mwra_referral_followup
            left join mwra_case using (case_name)
        where referral_followup_id not in
            (select referral_followup_id from get_case_details_using_case_name)
    ),

    union_records as (
        select * from get_case_details_using_case_name
        union all
        select * from get_case_details_using_woman_id
    ),

    add_status_fields as (
        select
            *,
            case
                when
                    status_treatment
                    in ('Service_availed', 'Currently_availing')
                then 'Yes'
                else 'No'
            end as availed_status,
            case
                when
                    accessref in (
                        'Yes_referred_place', 'Yes_another_hospital', 'Hospital_Refusal'
                    )
                then 'Yes'
                else 'No'
            end as accessed_status
        from union_records
    )


select *
from add_status_fields
