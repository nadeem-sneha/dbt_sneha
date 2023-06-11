
-- select count(*) as failures, count(*) != 0 as should_warn, count(*) != 0 as should_error
-- from
--     (
        with
            left_table as (
                select case_id as id, woman_name
                from "sneha_common"."dev_goalkeep"."mwra_visits_normalized"
                where case_id is not null and 1 = 1
            ),

            right_table as (
                select case_id as id
                from "sneha_common"."dev_goalkeep"."mwra_case_normalized"
                where case_id is not null and 1 = 1
            ),
            exceptions as (
                select left_table.id, left_table.woman_name, right_table.id as right_id
                from left_table
                left join right_table on left_table.id = right_table.id
                where right_table.id is null
            )

        select *
        from exceptions

    -- ) dbt_internal_test
