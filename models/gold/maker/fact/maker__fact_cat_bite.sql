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
    ilk,
    urn_address,
    art / pow(
        10,
        token_decimals
    ) AS art,
    ink / pow(
        10,
        token_decimals
    ) AS ink,
    tab,
    id
FROM
    {{ ref('silver_maker__cat_bite') }}
    LEFT JOIN {{ ref('silver_maker__decimals') }}
    ON LEFT(ilk, POSITION('-', ilk) - 1) = token_symbol
