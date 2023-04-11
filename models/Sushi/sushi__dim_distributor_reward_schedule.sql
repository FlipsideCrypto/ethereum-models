{{ config(
    materialized = 'table',
    enabled = false,
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
        ) }}

select merkle_root,
        case when merkle_root = '0x015e6c2cd1a4d6fa77aa1884c436b2435aae4beab5c9a091f18fd0c00dc7e577' then cast('2021-10-22 23:10:00' as timestamp_ntz)
             when merkle_root = '0x2828440576d787582da1af59d0bc0f090e5add2680610dc2ffa53b5a6aa30b07' then cast('2021-10-12 21:47:00' as timestamp_ntz)
             end as rewards_snapshot,
        c.key as address,
        c.value:index::number as index,
        silver.js_hex_to_int(
        c.value :amount :: STRING
    ) / pow(10,18) AS amount,
       c.value:proof::array as proof
from {{ source(
        'bronze_streamline',
        'sushi_rewards_schedule'
    ) }},
table(flatten(claims)) c
