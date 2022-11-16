{{ config(
    materialized = 'view'
) }}

SELECT
    hour,
    token_address,
    c.symbol,
    c.decimals,
    price,
    is_imputed
FROM {{ ref('silver__token_prices_all_providers_hourly') }} p
LEFT JOIN {{ ref('core__dim_contracts') }} c 
    ON LOWER(c.address) = LOWER(p.token_address)
QUALIFY(ROW_NUMBER() OVER(PARTITION BY hour, token_address ORDER BY priority ASC)) = 1
