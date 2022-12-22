{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}



select
           _airbyte_ab_id,
           _airbyte_data ->> 'id' as id,
           _airbyte_data ->> 'properties_womanid' as womanid,
           _airbyte_data ->> 'properties_womanname' as womanname,
    	   _airbyte_data ->>  'properties_clusterid' as clusterid,
    	   _airbyte_data ->>  'properties_center' as center,
    	   _airbyte_data ->> 'properties_coid' as coid,
    	   (_airbyte_data ->>  'closed')::boolean as closed,
    	   _airbyte_data ->>  'properties_anc_closereason' as anc_closereason,
    	   _airbyte_data ->>  'properties_high_risk_preg' as high_risk_preg,
    	   date(_airbyte_data ->>  'properties_lmpdate') as lmpdate,
    	   _airbyte_data ->>  'properties_referral' as referral,
    	   date(_airbyte_data ->>  'properties_referraldate') as referral_date,
    	   _airbyte_data ->>  'properties_referralplace' as referral_place,
    	   _airbyte_data ->>  'properties_referralreason' as referral_reason,
    	   _airbyte_data ->>  'properties_referralcategory' as referral_category,
    	   _airbyte_data ->>  'properties_prev_pregoutcome' as prev_pregoutcome,
           date(_airbyte_data ->> 'properties_finalWDOB') as  finalwdob,
           (_airbyte_data ->> 'properties_gravida_count')::int as  gravida_count,
           _airbyte_data ->> 'properties_referral_followupname' as  referral_followupname

from {{ source('source_commcare', 'zzz') }}
