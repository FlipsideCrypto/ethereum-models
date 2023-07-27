{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MAKER, MKR',
    'PURPOSE': 'GOVERNANCE, DEFI' }} }
) }}

SELECT
    tx_hash,
    event_index,
    block_number,
    block_timestamp,
    contract_address,
    origin_from_address,
    origin_to_address,
    usr,
    cdp,
    dink,
    dart
FROM
    {{ ref('silver_maker__cdp_frob') }}
