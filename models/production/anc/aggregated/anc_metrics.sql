{{ config(
  materialized='table'
) }}

{{ dbt_utils.union_relations(
    relations=[ref('anc_metrics_all_tmp'), ref('anc_metrics_clustered_tmp')],
    exclude=[]
) }}

