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
  vld.id,
  vld.voltrain,
  vld.interaction,
  vld.meetingtypes,
  vld.meetingdate
FROM {{ref('volunteer_case_normalized')}} as vcn
join {{ref('volunteer_training_normalized')}} as vld
on vcn.id = vld.case_id