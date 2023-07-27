{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MAKER, MKR',
    'PURPOSE': 'GOVERNANCE, DEFI' } } },
    tags = ['non_realtime']
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
    amount_delegated
FROM
    {{ ref('silver_maker__delegations') }}
