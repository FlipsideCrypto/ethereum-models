{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MAKER, MKR',
    'PURPOSE': 'DEFI' } } },
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    vc.block_number,
    vc.block_timestamp,
    vc.tx_hash,
    vc.vault_no AS vault_number,
    TRIM(
        vc.ilk
    ) AS collateral_type,
    urn_address AS urn_address,
    vault,
    owner_address AS creator,
    COALESCE (
        vc.vault_creation_id,
        {{ dbt_utils.generate_surrogate_key(
            ['vc.vault']
        ) }}
    ) AS ez_vault_creation_id,
    GREATEST(
        vc.inserted_timestamp,
        u.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    GREATEST(
        vc.modified_timestamp,
        u.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_maker__vault_creation') }}
    vc
    JOIN {{ ref('silver_maker__urns') }}
    u
    ON vc.vault_no = u.vault_no
