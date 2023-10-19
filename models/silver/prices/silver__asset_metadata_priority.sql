{{ config(
    materialized = 'incremental',
    unique_key = 'token_address',
    tags = ['non_realtime']
) }}

SELECT
    p.token_address,
    p.id,
    COALESCE(
        C.symbol,
        p.symbol
    ) AS symbol,
    C.name,
    C.decimals,
    p.provider,
    CASE
        WHEN p.provider = 'coingecko' THEN 1
        WHEN p.provider = 'coinmarketcap' THEN 2
    END AS priority,
    p._inserted_timestamp
FROM
    {{ ref('bronze__asset_metadata_priority') }}
    p
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON LOWER(
        C.address
    ) = p.token_address
WHERE
    1 = 1

{% if is_incremental() %}
AND p._inserted_timestamp >= (
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
