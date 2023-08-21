{{ config(
    materialized = 'incremental',
    unique_key = 'token_address',
    tags = ['non_realtime']
) }}

SELECT
    token_address,
    id,
    COALESCE(
        C.symbol,
        p.symbol
    ) AS symbol,
    NAME,
    decimals,
    provider,
    p._inserted_timestamp
FROM
    {{ ref('bronze__asset_metadata_all_providers') }}
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

qualify(ROW_NUMBER() over (PARTITION BY token_address, id, COALESCE(C.symbol, p.symbol), provider
ORDER BY
    p._inserted_timestamp DESC)) = 1
