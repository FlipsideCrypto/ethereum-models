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
    ilk,
    urn_address,
    art,
    ink,
    due,
    clip,
    id
FROM
    {{ ref('silver_maker__dog_bark') }}
