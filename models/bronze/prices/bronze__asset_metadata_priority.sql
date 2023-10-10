{{ config (
    materialized = 'view'
) }}

SELECT
    token_address,
    symbol,
    provider,
    id,
    _inserted_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'asset_metadata_priority'
    ) }}
WHERE
    blockchain = 'ethereum'
