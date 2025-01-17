{{ config(
    materialized = 'table',
    cluster_by = ['block_number'],
    tags = ['token_balances_partition']
) }}

SELECT 
    block_number,
    block_timestamp,
    address,
    contract_address,
    balance,
    _inserted_timestamp
FROM {{ ref('silver__token_balances') }}
WHERE block_number >= 5000000
AND block_number < 10000000