{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}



select
           _airbyte_ab_id,
           _airbyte_data -> 'properties_womanid' as womanid,
    	   _airbyte_data ->  'properties_clusterid' as clusterid,
    	   _airbyte_data ->  'properties_center' as center,
    	   _airbyte_data ->  'properties_coid' as coid,
    	   _airbyte_data ->  'closed' as closed,
    	   _airbyte_data ->  'properties_anc_closereason' as anc_closereason,
    	   _airbyte_data ->  'properties_high_risk_preg' as high_risk_preg,
    	   _airbyte_data ->  'properties_lmpdate' as lmpdate,
    	   _airbyte_data ->  'properties_referral' as referral,
    	   _airbyte_data ->  'properties_referraldate' as referral_date,
    	   _airbyte_data ->  'properties_referralplace' as referral_place,
    	   _airbyte_data ->  'properties_referralreason' as referral_reason,
    	   _airbyte_data ->  'properties_referralcategory' as referral_category,
    	   _airbyte_data ->  'properties_prev_pregoutcome' as prev_pregoutcome

from {{ source('source_commcare', 'zzz') }}
