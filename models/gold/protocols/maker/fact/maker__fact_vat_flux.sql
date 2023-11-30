{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MAKER, MKR',
    'PURPOSE': 'GOVERNANCE, DEFI' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    origin_from_address,
    origin_to_address,
    ilk,
    receiver,
    sender,
    wad,
    COALESCE (
        vat_flux_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS fact_vat_flux_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_maker__vat_flux') }}
