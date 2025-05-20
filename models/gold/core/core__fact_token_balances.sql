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
    tags = ['gold','balances']
) }}

SELECT
    block_number,
    block_timestamp,
    address AS user_address,
    contract_address,
    balance,
    COALESCE (
        token_balances_id,
        {{ dbt_utils.generate_surrogate_key(
             ['block_number', 'address','contract_address']
        ) }}
    ) AS fact_token_balances_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_balances') }}
