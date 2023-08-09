{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'MAKER, MKR',
    'PURPOSE': 'DEFI' } } }
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
    owner_address AS creator
FROM
    {{ ref('silver_maker__vault_creation') }}
    vc
    JOIN {{ ref('silver_maker__urns') }}
    u
    ON vc.vault_no = u.vault_no
