{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['token_address','symbol','id','provider'],
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
    p._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address','symbol','id','provider']
    ) }} AS asset_metadata_all_providers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
