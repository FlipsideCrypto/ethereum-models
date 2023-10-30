{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_number AS slot_number,
    state_id,
    INDEX,
    balance
FROM
    {{ ref('silver__beacon_validators') }}