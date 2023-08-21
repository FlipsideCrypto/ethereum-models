{{ config (
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address,
    blockchain,
    provider,
    price,
    is_imputed,
    _inserted_timestamp,
    _unique_key
FROM
    {{ source(
        'crosschain_silver',
        'token_prices_all_providers_hourly'
    ) }}
WHERE
    blockchain = 'ethereum'
