{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    id,
    symbol,
    NAME,
    decimals,
    COALESCE (
        asset_metadata_priority_id,
        {{ dbt_utils.generate_surrogate_key(
            ['token_address']
        ) }}
    ) AS ez_asset_metadata_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__asset_metadata_priority') }}