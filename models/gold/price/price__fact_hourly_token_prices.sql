{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    price,
    is_imputed,
    provider
FROM
    {{ ref('silver__hourly_prices_all_providers') }}
