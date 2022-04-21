{{ config(
    materialized = 'table',
    unique_key = 'address',
    ) }}

select c.key as address, 
       c.value:index::number as index, 
       c.value:amount::string as amount, 
       c.value:proof::array as proof
from {{ source(
        'sushi_external',
        'sushi_rewards_schedule'
    ) }},
table(flatten(claims)) c
