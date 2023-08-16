with
    cases as (select * from {{ ref('mwra_case_normalized') }}),
    visits as (select * from {{ ref("mwra_visit") }}),

    -- use row_number to get the last visit
    ordered_visits as (
        select
            visits.*,
            row_number() over (partition by case_id order by visit_date desc) as ov
        from visits
    ),
    last_visit as (
        select
            case_id,
            visit_date as last_mwra_visit_date,
            fpvisitreason as last_visit_reason,
            visit_conducted_by as last_visit_conducted_by,
            fp_eligible,
            fp_conversion
        from ordered_visits
        where ov = 1
    ), 
    -- get last visit with fp status
    ordered_visits_fp_last as (
        select
            visits.*,
            row_number() over (partition by case_id order by visit_date desc) as ov
        from visits
        where
            visits.fp is not null
            and visits.fpvisitreason = 'Family_planning'
    ),
    last_non_null_fp_visit as (
        select
            case_id,
            fp as last_known_fp,
            visit_date
        from ordered_visits_fp_last where ov = 1
    ), 
    -- get last visit with non-null fpmethod
    ordered_visits_fpmethod_last as (
        select
            visits.*,
            row_number() over (partition by case_id order by visit_date desc) as ov
        from visits
        where
            visits.fpmethod is not null
            and visits.fpvisitreason = 'Family_planning'
    ),
    last_non_null_fpmethod_visit as (
        select
            case_id,
            fpmethod as last_known_fpmethod,
            visit_date
        from ordered_visits_fpmethod_last
        where ov = 1
    ),
    -- get last visit with non-null MSeligible
    ordered_visits_fp_eligible as (
        select
            visits.*,
            row_number() over (partition by case_id order by visit_date desc) as ov
        from visits
        where
            visits.fp_eligible is not null
            and visits.fpvisitreason = 'Family_planning'
    ),
    last_non_null_fp_eligible_visit as  (
        select
            case_id,
            fp_eligible as last_known_fp_eligible,
            visit_date
        from ordered_visits_fp_eligible
        where ov = 1
    ), 
    -- get last visit with non-null fpmethodnot
    ordered_visits_fpmethodnot as (
        select
            visits.*,
            row_number() over (partition by case_id order by visit_date desc) as ov
        from visits
        where
            visits.fpmethodnot is not null
            and visits.fpvisitreason = 'Family_planning'
    ),
    last_non_null_fpmethodnot_visit as  (
        select
            case_id,
            fpmethodnot as last_known_fpmethodnot,
            visit_date
        from ordered_visits_fpmethodnot
        where ov = 1
    ), 

    add_visit_dimensions as (
        select
            cases.*,
            last_mwra_visit_date,
            (current_date - last_mwra_visit_date) as days_from_last_visit,
            last_visit_reason,
            last_visit_conducted_by,
            case
                when
                    (
                        extract(month from last_mwra_visit_date)
                        = extract(month from current_date)
                    )
                    and (
                        extract(year from last_mwra_visit_date)
                        = extract(year from current_date)
                    )
                then 'Visited'
                else 'Not Yet Visited'
            end as current_month_visit_status,
            last_known_fp,
            case
                when last_known_fp = 'Yes'
                then last_known_fpmethod
                else 'NA'
            end as last_known_fpmethod,
            case
                when last_known_fp = 'No'
                then last_known_fpmethodnot
                else 'NA'
            end as last_known_fpmethodnot,
            last_known_fp_eligible,
            fp_eligible,
            fp_conversion
        from cases
            left join last_visit using (case_id)
            left join last_non_null_fp_visit using (case_id)
            left join last_non_null_fpmethod_visit using (case_id)
            left join last_non_null_fp_eligible_visit using (case_id)
            left join last_non_null_fpmethodnot_visit using (case_id)
    )

select
    *
from add_visit_dimensions