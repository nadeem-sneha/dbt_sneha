with
    case_counts as (
        select
            'cases' as table_name,
            'case_id' as primary_key,
            count(*) as intermediate_rows,
            count(
                case when case_id is null then case_id else null end
            ) as rows_with_null_pk,
            null as foreign_key,
            cast(null as bigint) as rows_without_foreign_key
        from {{ ref("mwra_case_normalized") }}
    ),

    visit_counts as (
        select
            'visits' as table_name,
            'visit_id (id)' as primary_key,
            count(*) as intermediate_rows,
            count(
                case when visit_id is null then visit_id else null end
            ) as rows_with_null_pk,
            'case_id' as foreign_key,
            count(
                case when case_id is null then visit_id else null end
            ) as rows_without_foreign_key
        from {{ ref("mwra_visits_normalized") }}
    ),

    referral_counts as (
        select
            'referral_followups' as table_name,
            'referral_id (case_id)' as primary_key,
            count(*) as intermediate_rows,
            count(
                case when referral_followup_id is null then 1 else null end
            ) as rows_with_null_pk,
            'woman_id' as foreign_key,
            count(
                case when woman_id is null then 1 else null end
            ) as rows_without_foreign_key
        from {{ ref("mwra_referral_followup_normalized") }}
    ),

    case_counts_prod as (
        select
            'cases' as table_name,
            count(*) as production_rows
        from {{ ref('mwra_case') }}
    ),

    visit_counts_prod as (
        select
            'visits' as table_name,
            count(*) as production_rows
        from {{ ref('mwra_visit') }}
    ),

    referral_followup_counts_prod as (
        select
            'referral_followups' as table_name,
            count(*) as production_rows
        from {{ ref('mwra_referral_followup') }}
    ),

    union_intermediate_counts as (
        select * from case_counts
        union all
        select * from visit_counts
        union all
        select * from referral_counts
    ),

    union_prod_counts as (
        select * from case_counts_prod
        union all
        select * from visit_counts_prod
        union all
        select * from referral_followup_counts_prod
    )

select
    table_name,
    intermediate_rows,
    production_rows,
    *
from union_intermediate_counts
    left join union_prod_counts using (table_name)