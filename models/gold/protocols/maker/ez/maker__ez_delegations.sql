{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MAKER, MKR',
    'PURPOSE': 'GOVERNANCE, DEFI' } } },
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_status,
    origin_from_address,
    contract_address,
    tx_event,
    delegate,
    amount_delegated_unadjusted,
    18 AS decimals,
    amount_delegated,
    COALESCE (
        delegations_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_delegations_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_maker__delegations') }}
