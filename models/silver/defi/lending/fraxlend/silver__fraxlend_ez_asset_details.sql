{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['non_realtime'],
) }}

WITH logs AS (

    SELECT
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] = '0xb7f7e57b7bb3a5186ad1bd43405339ba361555344aec7a4be01968e88ee3883e' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
            WHEN topics [0] = '0x9303649990c462969a3c46d4e2c758166e92f5a4b18c67f26d3e58d2b0660e67' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42))
            WHEN topics [0] = '0xc6fa598658c9cdf9eaa5f76414ef17a38a7f74c0e719a0571a3f73d9ecd755b7' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42))
        END AS pool_address,
        *
    FROM
        {{ ref('silver__logs') }}
    WHERE
        origin_from_address IN (
            '0xdb3388e770f49a604e11f1a2084b39279492a61f',
            '0xf4e1d185666a624099298fcc42c50ba662dc7e52',
            '0xaa913c26dd7723fcae9dbd2036d28171a56c6251'
        )
        AND
         topics [0] IN (
            '0xb7f7e57b7bb3a5186ad1bd43405339ba361555344aec7a4be01968e88ee3883e',
            '0x9303649990c462969a3c46d4e2c758166e92f5a4b18c67f26d3e58d2b0660e67',
            '0xc6fa598658c9cdf9eaa5f76414ef17a38a7f74c0e719a0571a3f73d9ecd755b7'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
logs_transform AS (
    SELECT
        pool_address AS frax_market_address,
        NAME AS frax_market_name,
        symbol AS frax_market_symbol,
        decimals,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 42)) AS underlying_asset,
        l._log_id,
        l._inserted_timestamp
    FROM
        logs l
        LEFT JOIN ethereum_dev.silver.contracts
        ON address = pool_address
)
SELECT
    frax_market_address,
    frax_market_name,
    frax_market_symbol,
    decimals,
    underlying_asset,
    _log_id,
    _inserted_timestamp
FROM
    logs_transform
WHERE
    frax_market_name <> null