{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    state_id,
    validator_aggregates,
    validators
FROM
    {{ ref('silver__beacon_sync_committees') }}