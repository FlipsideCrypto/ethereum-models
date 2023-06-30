{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['beacon']
) }}

SELECT
    block_number,
    state_id,
    INDEX,
    balance
FROM
    {{ ref('silver__beacon_validators') }}