{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour'],
    tags = ['non_realtime']
) }}

SELECT
    HOUR,
    token_address,
    provider,
    price,
    is_imputed,
    _inserted_timestamp
FROM
    {{ ref('bronze__hourly_prices_all_providers') }}
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