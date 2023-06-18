{{ config(
  materialized='view',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

with referral_cte as (SELECT 
_airbyte_data ->> 'id' as id,
_airbyte_data -> 'properties' ->> 'case_name' as case_name,
_airbyte_data -> 'properties' ->> 'womanname' as womanname,
_airbyte_data -> 'properties' ->> 'womanid' as womanid,
(_airbyte_data -> 'closed')::boolean as referral_closed,
date(NULLIF(_airbyte_data -> 'properties' ->> 'followupdate','')) as followup_date, 
_airbyte_data -> 'properties' ->> 'statustreatment' as status_treatment,
_airbyte_data -> 'properties' ->> 'referralcategory' as referral_category,
date(NULLIF(_airbyte_data -> 'properties' ->> 'referraldate','')) as referral_date,
COALESCE(NULLIF(_airbyte_data -> 'properties' ->> 'referral_placecat1',''),
NULLIF(_airbyte_data -> 'properties' ->> 'referral_placecat2',''),
NULLIF(_airbyte_data -> 'properties' ->> 'referral_placecat3','')) as referral_place,
COALESCE(NULLIF(_airbyte_data -> 'properties' ->> 'referralreason1',''),
NULLIF(_airbyte_data -> 'properties' ->> 'referralreason2',''),
NULLIF(_airbyte_data -> 'properties' ->> 'referralreason3','')) as referral_reason,
_airbyte_data -> 'properties' ->> 'followup' as followup_required,
CASE WHEN date(NULLIF(_airbyte_data -> 'properties' ->> 'followupdate','')) IS NOT NULL THEN 'Yes'
ELSE 'No'
END AS referral_followed_up,
_airbyte_ab_id,
_airbyte_emitted_at
from {{ source('commcare_common', 'raw_case') }}
where (_airbyte_data -> 'properties' ->> 'case_type') = 'sneharefollwuptemp'
AND (_airbyte_data -> 'properties' ->> 'referralcategory') = 'ANC/PNC' 
/*removing test cases */
AND (_airbyte_data -> 'properties' ->> 'womanname') NOT LIKE '%Demo%'
AND (_airbyte_data -> 'properties' ->> 'womanname') NOT LIKE '%dummy%'
AND (_airbyte_data -> 'properties' ->> 'womanname') NOT LIKE '%error%'
)

{{ dbt_utils.deduplicate(
    relation='referral_cte',
    partition_by='id',
    order_by='_airbyte_emitted_at desc',
   )
}}