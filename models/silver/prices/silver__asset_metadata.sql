{{ config(
    materialized = 'incremental',
    unique_key = 'token_address'
) }}

SELECT
    token_address,
    symbol,
    provider,
    id,
    CASE
        WHEN provider = 'coingecko' THEN 1
        WHEN provider = 'coinmarketcap' THEN 2
    END AS priority,
    _inserted_timestamp
FROM
    {{ ref('bronze__asset_metadata') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address
ORDER BY
    priority ASC)) = 1
