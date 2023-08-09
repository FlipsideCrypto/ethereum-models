{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'BALANCES'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    address AS user_address,
    balance
FROM
    {{ ref('silver__eth_balances') }}
