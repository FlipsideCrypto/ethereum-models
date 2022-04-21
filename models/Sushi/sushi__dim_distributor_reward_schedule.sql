{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    ) }}

select c.key as address, 
       c.value:index::number as index, 
        silver.js_hex_to_int(
        c.value :amount :: STRING
    ) / pow(10,18) AS amount, 
       c.value:proof::array as proof
from {{ source(
        'sushi_external',
        'sushi_rewards_schedule'
    ) }},
table(flatten(claims)) c
