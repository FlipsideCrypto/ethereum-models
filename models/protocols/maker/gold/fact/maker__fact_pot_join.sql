{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    event_index,
    block_number,
    block_timestamp,
    contract_address,
    origin_from_address,
    origin_to_address,
    wad,
    usr
FROM
    {{ ref('silver_maker__pot_join') }}
