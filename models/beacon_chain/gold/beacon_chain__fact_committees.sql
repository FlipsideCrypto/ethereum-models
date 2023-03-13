{{ config(
    materialized = 'view',
    enabled = false,
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    state_id,
    INDEX,
    slot,
    validators
FROM
    {{ ref('silver__beacon_committees') }}