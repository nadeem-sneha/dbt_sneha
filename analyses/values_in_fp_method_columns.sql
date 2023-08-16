with
    visits as (select * from {{ ref('mwra_visit') }})

select
    distinct fpmethod
from
    visits
order by 1