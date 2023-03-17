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
        _airbyte_data -> 'properties' ->> 'womanid' as womanid,
        _airbyte_data -> 'properties' ->> 'person_organization_id' as person_organizaton_id,
        _airbyte_data -> 'properties' ->> 'womanname' as womanname,
        _airbyte_data -> 'properties' ->> 'age' as  age,
    	  _airbyte_data -> 'properties' ->> 'cluster_id' as clusterid,
        _airbyte_data -> 'properties' ->> 'cluster_name' as clustername,
    	  _airbyte_data -> 'properties' ->> 'co_id' as coid,
        _airbyte_data -> 'properties' ->> 'program_code' as program_code,
        _airbyte_data -> 'properties' ->> 'program_name' as program_name,
        _airbyte_data -> 'properties' ->> 'hh_number' as hh_number,
        _airbyte_data -> 'properties' ->> 'aww_number' as aww_number,
    	  (_airbyte_data ->  'closed')::boolean as closed,
        _airbyte_data -> 'properties' ->> 'anc_enrolled' as anc_enrolled,
        _airbyte_data -> 'properties' ->> 'ancreg' as anc_reg,
        _airbyte_data -> 'properties' ->> 'anc_close' as anc_closed,
    	  _airbyte_data -> 'properties' ->> 'anc_closereason' as anc_closereason,
    	  _airbyte_data -> 'properties' ->> 'high_risk_preg' as high_risk_preg,
        _airbyte_data -> 'properties' ->> 'why_high_risk' as  why_high_risk,
        _airbyte_data -> 'properties' ->> 'hb_grade' as  hb_grade,
    	  COALESCE(date(NULLIF(_airbyte_data -> 'properties' ->> 'lmp','')),date(NULLIF(_airbyte_data -> 'properties' ->> 'edd',''))-280) as lmpdate,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'edd','')) as edddate,
        (_airbyte_data -> 'properties' ->> 'total_gravida')::int as  gravida_count,
        _airbyte_data -> 'properties' ->> 'pregoutcome' as  pregoutcome,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'deliverydate','')) as  delivery_date,
        _airbyte_data -> 'properties' ->> 'deliverysite' as  delivery_site,
        _airbyte_data -> 'properties' ->> 'case_type' as  case_type,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'date_opened','')) as  case_opened_date,
        _airbyte_data -> 'properties' ->> 'individual_category' as  individual_category,
        _airbyte_data -> 'properties' ->> 'service_registration' as  service_registration,
        date(NULLIF(_airbyte_data -> 'properties' ->> 'identifydate','')) as  anc_identify_date,
        date(_airbyte_data ->> 'date_closed'::text) as  date_closed
from {{ source('commcare_anc', 'raw_case') }}

where (_airbyte_data -> 'properties' ->> 'case_type') = 'case' 
AND (_airbyte_data -> 'properties' ->> 'individual_category') = 'mwra'
AND (_airbyte_data -> 'properties' ->> 'anc_enrolled' IS NOT NULL)
/*removing test cases */
AND (_airbyte_data -> 'properties' ->> 'womanname') NOT LIKE '%Demo%'
