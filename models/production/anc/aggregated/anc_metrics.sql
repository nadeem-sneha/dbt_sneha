{{ config(
  materialized='table'
) }}

{{ dbt_utils.union_relations(
    relations=[ref('anc_metrics_calculated_all_tmp'), ref('anc_metrics_calculated_clustered_tmp')],
    exclude=[]
) }}

