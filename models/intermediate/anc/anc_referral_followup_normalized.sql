{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

SELECT 
(_airbyte_data ->> 'id') as id,
date(NULLIF(_airbyte_data -> 'form' ->> 'followupdate','')) as followup_date, 
(_airbyte_data -> 'form' ->> 'statustreatment') as status_treatment,
_airbyte_data -> 'form' ->> 'womanid' as womanid,
_airbyte_data -> 'form' ->> 'referralcategory' as referral_category,
date(NULLIF(_airbyte_data -> 'form' ->> 'referraldate','')) as referral_date,
_airbyte_data -> 'form' ->> 'referral_placecat1' AS referral_place,
_airbyte_data -> 'form' ->> 'referral_reasoncat1' as referral_reason,
_airbyte_data -> 'form' ->> 'followup' as followup_required,
_airbyte_ab_id,
_airbyte_emitted_at
from {{ source('commcare_anc', 'raw_referral_followup') }} 
WHERE
(_airbyte_data -> 'form' ->> 'referralcategory') = 'ANC/PNC' 
AND (_airbyte_data ->> 'archived')::boolean = false
/*remove test data */
AND  (_airbyte_data -> 'form' ->> 'coid') NOT IN ('00','001') 
AND  (_airbyte_data -> 'form' ->> 'coid') <> ''