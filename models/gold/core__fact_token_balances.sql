{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    address AS user_address,
    contract_address,
    balance
FROM
    {{ ref('silver__token_balances') }}
