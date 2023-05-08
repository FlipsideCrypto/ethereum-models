{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour']
) }}

SELECT
    p.hour,
    p.token_address,
    p.price,
    p.is_imputed,
    p._inserted_timestamp,
    COALESCE(
        C.token_symbol,
        m.symbol
    ) AS symbol,
    C.token_decimals AS decimals
FROM
    {{ ref('silver__hourly_prices') }}
    p
    LEFT JOIN {{ ref('silver__asset_metadata') }}
    m
    ON p.token_address = m.token_address
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON p.token_address = C.contract_address
WHERE
    1 = 1

{% if is_incremental() %}
AND p._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
