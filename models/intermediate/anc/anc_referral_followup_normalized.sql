{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

SELECT 
_airbyte_data ->> 'id' as id,
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
from {{ source('commcare_anc', 'raw_case') }}
where (_airbyte_data -> 'properties' ->> 'case_type') = 'sneharefollwuptemp'
AND (_airbyte_data -> 'properties' ->> 'referralcategory') = 'ANC/PNC' 
/*remove test data */
AND  (_airbyte_data -> 'properties' ->> 'coid') NOT IN ('00','001') 
AND  (_airbyte_data -> 'properties' ->> 'coid') <> ''
