{{ config(
  materialized='table'
) }}

{{ dbt_utils.union_relations(
    relations=[ref('anc_metrics_pct_calculated_all_tmp'), ref('anc_metrics_pct_calculated_clustered_tmp')],
    exclude=[]
) }}

