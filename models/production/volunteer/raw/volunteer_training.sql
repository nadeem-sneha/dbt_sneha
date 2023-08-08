{{ config(
  materialized='table'
) }}

--generating final table
select  
  vcn.id as case_id,
  vcn.clustername,
  vcn.program_name,
  vcn.co_id_from_case as co_id,
  vcn.aww_number,
  vcn.hh_number,
  vcn.person_name,
  vcn.sex,
  vtn.id,
  vtn.voltrain,
  vtn.interaction,
  vtn.meeting_type,
  vtn.meeting_date 
FROM {{ref('volunteer_case_normalized')}} as vcn
join {{ref('volunteer_training_normalized')}} as vtn
on vcn.id = vtn.case_id