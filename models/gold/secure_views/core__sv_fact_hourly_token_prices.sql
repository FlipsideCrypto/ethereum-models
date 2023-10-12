{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_FACT_HOURLY_TOKEN_PRICES', 'VIEW') }}"
) }}

SELECT
    hour,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ ref('silver__hourly_prices_priority') }}
