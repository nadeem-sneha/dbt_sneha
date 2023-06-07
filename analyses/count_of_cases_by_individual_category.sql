with 
    cases as (select * from {{ ref('anc_case_normalized') }})

select individual_category, count(individual_category)
from cases
group by individual_category 