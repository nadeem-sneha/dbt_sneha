{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data ->> 'id' as id,
        _airbyte_data ->> 'properties_womanid' as womanid,
        _airbyte_data ->> 'properties_person_organization_id' as person_organizaton_id,
        _airbyte_data ->> 'properties_womanname' as womanname,
        _airbyte_data ->> 'properties_age' as  age,
    	  _airbyte_data ->>  'properties_cluster_id' as clusterid,
        _airbyte_data ->>  'properties_clustername' as clustername,
    	  _airbyte_data ->> 'properties_coid' as coid,
        _airbyte_data ->> 'properties_program_code' as program_code,
        _airbyte_data ->> 'properties_program_name' as program_name,
        _airbyte_data ->> 'properties_hh_number' as hh_number,
        _airbyte_data ->> 'properties_aww_number' as aww_number,
    	  (_airbyte_data ->>  'closed')::boolean as closed,
        _airbyte_data ->>  'properties_anc_enrolled' as anc_enrolled,
        _airbyte_data ->>  'properties_ancreg' as anc_reg,
        _airbyte_data ->>  'properties_anc_close' as anc_closed,
    	  _airbyte_data ->>  'properties_anc_closereason' as anc_closereason,
    	  _airbyte_data ->>  'properties_high_risk_preg' as high_risk_preg,
        _airbyte_data ->> 'properties_why_high_risk' as  why_high_risk,
        /*_airbyte_data ->> 'properties_womengarde_cat' as  woman_bmi_grade,*/
        _airbyte_data ->> 'properties_hb_grade' as  woman_bmi_grade,
    	  date(NULLIF(_airbyte_data ->>  'properties_lmp','')) as lmpdate,
        date(NULLIF(_airbyte_data ->>  'properties_edd','')) as edddate,
    	  _airbyte_data ->>  'properties_referral' as referral,
    	  date(NULLIF(_airbyte_data ->>  'properties_referraldate','')) as referral_date,
    	  _airbyte_data ->>  'properties_referralplace1' AS referral_place,
    	  _airbyte_data ->>  'properties_referralreason1' as referral_reason,
        _airbyte_data ->>  'properties_referralreason1' as referral_reason1,
        _airbyte_data ->>  'properties_referralreason2' as referral_reason2,
        _airbyte_data ->>  'properties_referralreason3' as referral_reason3,
    	  _airbyte_data ->>  'properties_referralcategory' as referral_category,
        (_airbyte_data ->> 'properties_total_gravida')::int as  gravida_count,
        _airbyte_data ->> 'properties_pregoutcome' as  pregoutcome,
        date(NULLIF(_airbyte_data ->> 'properties_deliverydate','')) as  delivery_date,
        _airbyte_data ->> 'properties_deliverysite' as  delivery_site,
        _airbyte_data ->> 'properties_case_type' as  case_type,
        date(NULLIF(_airbyte_data ->> 'properties_date_opened','')) as  case_opened_date

from {{ source('commcare_anc', 'raw_case') }}
where (_airbyte_data ->> 'properties_case_type') = 'case' 
AND  (_airbyte_data ->> 'properties_coid') NOT IN ('00','001') 
AND  (_airbyte_data ->> 'properties_coid') <> ''
