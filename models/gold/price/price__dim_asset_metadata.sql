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
    provider,
    COALESCE (
        asset_metadata_all_providers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['token_address','symbol','id','provider']
        ) }}
    ) AS dim_asset_metadata_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__asset_metadata_all_providers') }}