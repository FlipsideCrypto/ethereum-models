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
    },
    tags = ['non_realtime']
) }}

SELECT
    block_number,
    block_timestamp,
    address AS user_address,
    contract_address,
    balance
FROM
    {{ ref('silver__token_balances') }}
