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
    origin_from_address,
    origin_to_address,
    usr1,
    usr2,
    wad
FROM
    {{ ref('silver_maker__dai_join_join') }}
